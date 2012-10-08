package com.codechronicle.aws.glacier.command;

import com.amazonaws.services.glacier.TreeHashGenerator;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.dao.FileUploadRecordDAO;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.codechronicle.aws.glacier.model.FileUploadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/22/12
 * Time: 3:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class PersistentUploadFileCommand extends GlacierCommand {

    private static Logger log = LoggerFactory.getLogger(PersistentUploadFileCommand.class);
    private String filePath;
    private String vault;

    public PersistentUploadFileCommand(EnvironmentConfiguration config) {
        super(config);
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
        FileUploadRecordDAO fuDAO = new FileUploadRecordDAO(getConfig().getDataSource());
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

        getResult().setResultCode(CommandResultCode.SUCCESS);
        getResult().setMessage("Successfully queued upload of file : " + file.getAbsolutePath());

        wakeWorkerThread();
    }

    private synchronized void wakeWorkerThread() throws InterruptedException {
        UploadFileWorker.startServicingUploadQueue(getConfig());
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
}
