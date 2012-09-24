package com.codechronicle.aws.glacier.command;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.codechronicle.aws.glacier.AppConstants;
import com.codechronicle.aws.glacier.dao.FileUploadPartDAO;
import com.codechronicle.aws.glacier.dao.FileUploadRecordDAO;
import com.codechronicle.aws.glacier.fileutil.FilePart;
import com.codechronicle.aws.glacier.model.FileUploadPart;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.codechronicle.aws.glacier.model.FileUploadStatus;
import com.mchange.v2.c3p0.PooledDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/22/12
 * Time: 3:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class PersistentUploadFileCommand extends GlacierCommand implements Runnable {

    private static Thread workerThread;
    private static Object workerThreadMonitor = new Object();

    private static Logger log = LoggerFactory.getLogger(PersistentUploadFileCommand.class);
    private String filePath;
    private String vault;

    public PersistentUploadFileCommand(Properties awsProperties, AmazonGlacier client, PooledDataSource dataSource) {
        super(awsProperties, client, dataSource);
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void setVault(String vault) {
        this.vault = vault;
    }

    @Override
    public void executeCustomLogic() throws Exception {

        File file = new File(filePath);

        validateInputFile(file);

        // First calculate treehash of entire file
        String fileHash = TreeHashGenerator.calculateTreeHash(file);
        log.info("Tree hash for " + file.getAbsolutePath() + " == " + fileHash);

        // Check database to see if we already have a record of this file, based on hash
        FileUploadRecordDAO fuDAO = new FileUploadRecordDAO(getDataSource());
        FileUploadRecord fileUploadRecord = fuDAO.findByHash(fileHash);

        if ((fileUploadRecord != null) && (this.vault.equals(fileUploadRecord.getVault()))) {

            // File upload entry exists,

            getResult().setResultCode(CommandResultCode.UPLOAD_ALREADY_EXISTS);
            getResult().setMessage("Upload entry already exists for vault=" + fileUploadRecord.getVault() + ", status = " + fileUploadRecord.getStatus());

            wakeWorkerThread();

            return;
        }

        // Create a record on the UPLOAD table as PENDING
        // Wake up file upload worker thread as needed

        fileUploadRecord = new FileUploadRecord();
        fileUploadRecord.setFilePath(file.getAbsolutePath());
        fileUploadRecord.setVault(vault);
        fileUploadRecord.setFileHash(fileHash);
        fileUploadRecord.setLength(file.length());
        fileUploadRecord.setStatus(FileUploadStatus.PENDING);


        log.info("Saving new file upload entry for " + file.getAbsolutePath());
        fuDAO.create(fileUploadRecord);

        wakeWorkerThread();

        // File upload worker thread:
        //  - When woken up, go into a loop of checking for new work to do based on UPLOAD table status
        //  - If there is no work to do, go to sleep blocking on a monitor
        //  - If new upload, initiate a new job id
        //      - Recalculate treehash at this point to make sure we pick up any changes
        //      - Update database record
        //      - Calculate and create all of the PART entries associated with this upload
        //      - Sequentially process part uploads
        //      - If all part uploads are finished, send upload complete via API
        //          - Compare final hash with recorded file hash
        //      - Update status for the upload file request in database
    }

    private synchronized void wakeWorkerThread() throws InterruptedException {
        if (workerThread == null) {
            workerThread = new Thread(this);
            workerThread.join();
            workerThread.start();
        } else {
            synchronized (workerThreadMonitor) {
                workerThreadMonitor.notify();
            }
        }
    }

    private void validateInputFile(File file) {
        if (!file.exists()) {
            getResult().setResultCode(CommandResultCode.FILE_NOT_FOUND);
            getResult().setMessage(file.getAbsolutePath());
            return;
        }

        if (!file.canRead()) {
            getResult().setResultCode(CommandResultCode.FILE_UNREADABLE);
            getResult().setMessage(file.getAbsolutePath());
            return;
        }
    }

    @Override
    public void run() {

        FileUploadRecordDAO fuDAO = new FileUploadRecordDAO(getDataSource());

        try {
            while (true) {

                // Start with in-progress uploads, FIFO servicing
                List<FileUploadRecord> inProgressFiles = fuDAO.findByStatus(FileUploadStatus.IN_PROGRESS);
                serviceUploads(inProgressFiles);

                // Then work on pending uploads, FIFO servicing
                List<FileUploadRecord> pendingFiles = fuDAO.findByStatus(FileUploadStatus.PENDING);
                serviceUploads(pendingFiles);

                // No more work to do right now, go to sleep and wait
                waitForMoreWork();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void serviceUploads(List<FileUploadRecord> uploadFiles) throws SQLException, IOException {
        for (FileUploadRecord uploadFile : uploadFiles) {
            log.info("Processing file : " + uploadFile.getFilePath());

            if (uploadFile.getStatus() == FileUploadStatus.PENDING) {
                processPendingUpload(uploadFile);
            } else if (uploadFile.getStatus() == FileUploadStatus.IN_PROGRESS) {
                processInProgressUpload(uploadFile);
            }
        }
    }

    private void processInProgressUpload(FileUploadRecord uploadFile) throws SQLException, IOException {
        processMultiPartUpload(uploadFile);
    }

    private void processPendingUpload(FileUploadRecord uploadFile) throws SQLException, IOException {

        //TODO: Do a direct upload if the size is smaller than the partition size
        String awsUploadId = initiateUpload(uploadFile);
        uploadFile.setAwsUploadId(awsUploadId);
        uploadFile.setStatus(FileUploadStatus.IN_PROGRESS);

        FileUploadRecordDAO fuDAO = new FileUploadRecordDAO(getDataSource());
        fuDAO.update(uploadFile);
        processMultiPartUpload(uploadFile);
    }

    private void processMultiPartUpload(FileUploadRecord uploadFile) throws SQLException, IOException {
        log.info("Continuing multi-part upload of file : " + uploadFile.getFilePath());

        // Look in parts table to determine the next part to upload
        FileUploadPartDAO fupDAO = new FileUploadPartDAO(getDataSource());
        int maxPartUploaded = fupDAO.findMaxSuccessfulPartNumber(uploadFile.getId());

        // Calculate number of parts that should be in the file
        int numTotalParts = calculateTotalParts(uploadFile);

        while (maxPartUploaded < numTotalParts) {
            uploadPart(uploadFile, ++maxPartUploaded);
        }

        // If done, then finish it out with the upload complete AWS API call.
    }

    private void uploadPart(FileUploadRecord uploadFile, int partNum) throws SQLException, IOException {
        FileUploadPart part = new FileUploadPart();
        part.setUploadId(uploadFile.getId());

        long startByte = (partNum-1) * AppConstants.NETWORK_PARTITION_SIZE;
        long endByte = startByte + AppConstants.NETWORK_PARTITION_SIZE - 1;

        if (endByte > uploadFile.getLength()) {
            endByte = uploadFile.getLength()- 1;
        }

        part.setStartByte(startByte);
        part.setEndByte(endByte);
        part.setPartNum(partNum);

        // Load the exact bytes that we need
        InputStream byteStream = null;

        try {
            byteStream = loadBytes(uploadFile.getFilePath(), startByte, endByte);
            part.setPartHash(TreeHashGenerator.calculateTreeHash(byteStream));
            byteStream.reset();
        } finally {
            IOUtils.closeQuietly(byteStream);
        }

        System.out.println("Part #" + partNum + " ==> Uploading bytes " + startByte + " to " + endByte);

        FileUploadPartDAO fupDao = new FileUploadPartDAO(getDataSource());
        fupDao.create(part);
    }

    private InputStream loadBytes(String filePath, long startByte, long endByte) throws IOException {
        int numBytes = (int)((endByte-startByte)+1);
        byte[] buffer = new byte[numBytes];

        RandomAccessFile raf = new RandomAccessFile(new File(filePath), "r");
        raf.seek(startByte);
        raf.read(buffer, 0, numBytes);

        System.out.println("Loading number of bytes = " + numBytes);
        System.out.println("Partition size = " + AppConstants.NETWORK_PARTITION_SIZE);

        ByteArrayInputStream is = new ByteArrayInputStream(buffer);
        return is;
    }

    private int calculateTotalParts(FileUploadRecord uploadFile) {

        int parts = (int)(uploadFile.getLength() / AppConstants.NETWORK_PARTITION_SIZE);
        int remainder = (int)(uploadFile.getLength() % AppConstants.NETWORK_PARTITION_SIZE);

        parts += (remainder > 0) ? 1 : 0;

        return parts;
    }

    private String initiateUpload(FileUploadRecord uploadFile) {
        String awsUploadId = "AWS-2343242";
        log.info("Initiating upload of file : " + uploadFile.getFilePath() + " with AWS upload id=" + awsUploadId);

        return awsUploadId;
    }

    private void waitForMoreWork() throws InterruptedException {
        synchronized (workerThreadMonitor) {
            workerThreadMonitor.wait();
        }
    }
}
