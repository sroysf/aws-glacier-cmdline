package com.codechronicle.aws.glacier.command;

import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.dao.FileUploadPartDAO;
import com.codechronicle.aws.glacier.dao.FileUploadRecordDAO;
import com.codechronicle.aws.glacier.model.FileUploadPart;
import com.codechronicle.aws.glacier.model.FileUploadRecord;

import java.util.List;

/**

 */
public class GetUploadDetailsCommand extends GlacierCommand {

    private int fileUploadId = -1;
    private float percentComplete = -1;
    private FileUploadRecord fileUploadRecord = null;
    private long lastByteUploaded = -1;

    List<FileUploadPart> completedParts = null;

    public GetUploadDetailsCommand(EnvironmentConfiguration config, int fileUploadId) {
        super(config);
        this.fileUploadId = fileUploadId;
    }

    @Override
    protected void executeCustomLogic() throws Exception {
        FileUploadRecordDAO fileDAO = new FileUploadRecordDAO(getConfig().getDataSource());
        fileUploadRecord = fileDAO.findById(fileUploadId);
        if (fileUploadRecord == null) {
            getResult().setResultCode(CommandResultCode.RECORD_NOT_FOUND);
            return;
        }

        FileUploadPartDAO dao = new FileUploadPartDAO(getConfig().getDataSource());
        this.completedParts = dao.findParts(fileUploadRecord);

        FileUploadPart lastPart = this.completedParts.get(completedParts.size()-1);
        lastByteUploaded = lastPart.getEndByte();
        percentComplete = (lastByteUploaded / fileUploadRecord.getLength()) * 100;
    }

    public List<FileUploadPart> getCompletedParts() {
        return completedParts;
    }

    public float getPercentComplete() {
        return percentComplete;
    }

    public FileUploadRecord getFileUploadRecord() {
        return fileUploadRecord;
    }

    public long getLastByteUploaded() {
        return lastByteUploaded;
    }
}
