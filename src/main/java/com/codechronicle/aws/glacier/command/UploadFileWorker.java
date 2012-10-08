package com.codechronicle.aws.glacier.command;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import com.codechronicle.aws.glacier.AppConstants;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.dao.FileUploadPartDAO;
import com.codechronicle.aws.glacier.dao.FileUploadRecordDAO;
import com.codechronicle.aws.glacier.event.Event;
import com.codechronicle.aws.glacier.event.EventRegistry;
import com.codechronicle.aws.glacier.event.EventType;
import com.codechronicle.aws.glacier.model.FileUploadPart;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.codechronicle.aws.glacier.model.FileUploadStatus;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/24/12
 * Time: 3:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class UploadFileWorker implements Runnable {

    private static Logger log = LoggerFactory.getLogger(UploadFileWorker.class);
    public static Object workerThreadMonitor = new Object();

    private EnvironmentConfiguration config;

    public UploadFileWorker(EnvironmentConfiguration config) {
        this.config = config;
    }

    @Override
    public void run() {

        FileUploadRecordDAO fuDAO = new FileUploadRecordDAO(config.getDataSource());

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
        if (uploadFile.getLength() < AppConstants.NETWORK_PARTITION_SIZE) {
            processEntireFileUpload(uploadFile);
        } else {
            processMultiPartUpload(uploadFile);
        }
    }

    private void processPendingUpload(FileUploadRecord uploadFile) throws SQLException, IOException {

        FileUploadRecordDAO fuDAO = new FileUploadRecordDAO(config.getDataSource());
        uploadFile.setStatus(FileUploadStatus.IN_PROGRESS);

        if (uploadFile.getLength() < AppConstants.NETWORK_PARTITION_SIZE) {
            fuDAO.update(uploadFile);
            processEntireFileUpload(uploadFile);
        } else {
            String awsUploadId = initiateUpload(uploadFile);
            uploadFile.setAwsUploadId(awsUploadId);
            fuDAO.update(uploadFile);
            processMultiPartUpload(uploadFile);
        }
    }

    private void processEntireFileUpload(FileUploadRecord uploadFile) throws IOException, SQLException {

        File file = new File(uploadFile.getFilePath());
        InputStream instream = null;

        String checksum = TreeHashGenerator.calculateTreeHash(file);
        long contentLength = uploadFile.getLength();

        try {
            byte[] buffer = FileUtils.readFileToByteArray(file);
            instream = new ByteArrayInputStream(buffer);
            UploadArchiveRequest request = new UploadArchiveRequest()
                    .withAccountId(config.getAccountId())
                    .withArchiveDescription(file.getName())
                    .withBody(instream)
                    .withChecksum(checksum)
                    .withContentLength(contentLength)
                    .withVaultName(uploadFile.getVault());

            UploadArchiveResult result = config.getClient().uploadArchive(request);
            if (!result.getChecksum().equals(checksum)) {
                throw new AmazonServiceException("Checksum mismatch. Client calculated : " + checksum + " but AWS computed : " + result.getChecksum());
            }

            uploadFile.setAwsArchiveId(result.getArchiveId());
            markFileUploadComplete(uploadFile);
        } finally {
            IOUtils.closeQuietly(instream);
        }
    }

    private void markFileUploadComplete(FileUploadRecord uploadFile) throws SQLException {
        FileUploadRecordDAO dao = new FileUploadRecordDAO(config.getDataSource());
        uploadFile.setStatus(FileUploadStatus.COMPLETE);
        uploadFile.setCompletionDate(new Timestamp(new GregorianCalendar().getTimeInMillis()));
        dao.update(uploadFile);

        Event event = new Event(EventType.UPLOAD_COMPLETE, uploadFile);
        EventRegistry.publish(event);
    }

    private void processMultiPartUpload(FileUploadRecord uploadFile) throws SQLException, IOException {
        log.info("Continuing multi-part upload of file : " + uploadFile.getFilePath());

        // Look in parts table to determine the next part to upload
        FileUploadPartDAO fupDAO = new FileUploadPartDAO(config.getDataSource());
        int maxPartUploaded = fupDAO.findMaxSuccessfulPartNumber(uploadFile.getId());

        // Calculate number of parts that should be in the file
        int numTotalParts = calculateTotalParts(uploadFile);

        while (maxPartUploaded < numTotalParts) {
            uploadPart(uploadFile, ++maxPartUploaded);
        }

        // If done, then finish it out with the upload complete AWS API call.
        verifyPartUploads(uploadFile);

        // Complete the upload via AWS API
        completeUpload(uploadFile);
    }

    private void completeUpload(FileUploadRecord uploadFile) throws SQLException {
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest();
        completeRequest.setAccountId(config.getAccountId());
        completeRequest.setVaultName(uploadFile.getVault());
        completeRequest.setArchiveSize("" + uploadFile.getLength());
        completeRequest.setChecksum(uploadFile.getFileHash());
        completeRequest.setUploadId(uploadFile.getAwsUploadId());
        completeRequest.setRequestCredentials(config.getCredentials());

        log.info("Completing file upload for AWS upload id = " + uploadFile.getAwsUploadId() + "[" + uploadFile.getFilePath() + "]");
        CompleteMultipartUploadResult result = config.getClient().completeMultipartUpload(completeRequest);
        String awsChecksum = result.getChecksum();
        if (!uploadFile.getFileHash().equals(awsChecksum)) {
            throw new RuntimeException("AWS checksum for full file did not match calculated checksum. AWS=" + awsChecksum + " Calculated=" + uploadFile.getFileHash());
        }

        uploadFile.setAwsArchiveId(result.getArchiveId());
        markFileUploadComplete(uploadFile);
        log.info("File upload complete : " + uploadFile.getFilePath());
    }

    private void verifyPartUploads(FileUploadRecord uploadFile) throws SQLException {
        FileUploadPartDAO fupDAO = new FileUploadPartDAO(config.getDataSource());
        List<FileUploadPart> parts = fupDAO.findParts(uploadFile);

        log.info("Verifying parts for " + uploadFile.getFilePath());
        long bytes = 0;
        for (FileUploadPart part : parts) {
            bytes += part.getEndByte() - part.getStartByte() + 1;
            log.info(part.getPartNum() + "[" + part.getStartByte() + "-" + part.getEndByte() + "]");
        }

        if (bytes != uploadFile.getLength()) {
            throw new RuntimeException("Byte count mismatch. Sum of parts = " + bytes + " bytes, but file length is " + uploadFile.getLength());
        }
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

        // Call Amazon API to upload the part.

        log.info("Part #" + partNum + " ==> Uploading bytes " + startByte + " to " + endByte);

        UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
                .withAccountId(config.getAccountId())
                .withBody(byteStream)
                .withChecksum(part.getPartHash())
                .withRange("bytes " + startByte + "-" + endByte + "/*")
                .withUploadId(uploadFile.getAwsUploadId())
                .withVaultName(uploadFile.getVault());

        UploadMultipartPartResult result = config.getClient().uploadMultipartPart(partRequest);

        if (!result.getChecksum().equals(part.getPartHash())) {
            throw new RuntimeException("Checksum mismatch. Client calculated : " + part.getPartHash() + " but AWS computed : " + result.getChecksum());
        }

        // Record the completed part in the database
        FileUploadPartDAO fupDao = new FileUploadPartDAO(config.getDataSource());
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

        File f = new File(uploadFile.getFilePath());
        String desc = f.getName();

        InitiateMultipartUploadRequest uploadJobRequest = new InitiateMultipartUploadRequest(uploadFile.getVault(), desc, ""+AppConstants.NETWORK_PARTITION_SIZE);
        InitiateMultipartUploadResult result = config.getClient().initiateMultipartUpload(uploadJobRequest);

        String awsUploadId = result.getUploadId();
        log.info("Initiating upload of file : " + uploadFile.getFilePath() + " with AWS upload id=" + awsUploadId);

        return awsUploadId;
    }

    //**********************************
    // Thread control

    private void waitForMoreWork() throws InterruptedException {
        synchronized (workerThreadMonitor) {
            workerThreadMonitor.wait();
        }
    }

    private static Thread workerThread = null;

    public static synchronized void startServicingUploadQueue(EnvironmentConfiguration config) throws InterruptedException {
        if (UploadFileWorker.workerThread == null) {
            UploadFileWorker.workerThread = new Thread(new UploadFileWorker(config));
            UploadFileWorker.workerThread.start();
        } else {
            synchronized (UploadFileWorker.workerThreadMonitor) {
                UploadFileWorker.workerThreadMonitor.notify();
            }
        }
    }
}
