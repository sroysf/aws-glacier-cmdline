package com.codechronicle.aws.glacier.command;

import com.codechronicle.aws.glacier.AppConstants;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.dao.FileUploadRecordDAO;
import com.codechronicle.aws.glacier.model.FileUploadRecord;

import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Use this command to get a listing of all files that have been uploaded to Amazon Glacier.
 *
 * This command operates on the local state database. It does not request an inventory from Amazon.
 */
public class ListUploadsCommand extends GlacierCommand {

    private List<FileUploadRecord> records = null;

    public ListUploadsCommand(EnvironmentConfiguration config) {
        super(config);
    }

    @Override
    protected void executeCustomLogic() throws Exception {
        FileUploadRecordDAO dao = new FileUploadRecordDAO(getConfig().getDataSource());
        records = dao.findAll();

        getResult().setResultCode(CommandResultCode.SUCCESS);
    }

    public List<FileUploadRecord> getRecords() {
        return records;
    }
}
