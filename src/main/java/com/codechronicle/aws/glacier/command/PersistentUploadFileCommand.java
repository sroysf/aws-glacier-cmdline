package com.codechronicle.aws.glacier.command;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.codechronicle.aws.glacier.dao.FileUploadRecordDAO;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.codechronicle.aws.glacier.model.FileUploadStatus;
import com.mchange.v2.c3p0.PooledDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

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

            //  - If pending, then just continue upload process
            //  - If complete, tell caller that it's already in Amazon Vault

            return;
        }

        // Create a record on the UPLOAD table as PENDING
        // Wake up file upload worker thread as needed

        fileUploadRecord = new FileUploadRecord();
        fileUploadRecord.setFileName(file.getName());
        fileUploadRecord.setVault(vault);
        fileUploadRecord.setFileHash(fileHash);
        fileUploadRecord.setStatus(FileUploadStatus.PENDING);

        log.info("Saving new file upload entry for " + file.getAbsolutePath());
        fuDAO.save(fileUploadRecord);


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
