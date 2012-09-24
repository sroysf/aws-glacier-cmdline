package com.codechronicle.aws.glacier.model;

import java.sql.Timestamp;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 5:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUploadPart {

    private int id;
    private int uploadId;
    private int partNum;
    private long startByte;
    private long endByte;
    private String partHash;
    private Timestamp completionDate;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getUploadId() {
        return uploadId;
    }

    public void setUploadId(int uploadId) {
        this.uploadId = uploadId;
    }

    public int getPartNum() {
        return partNum;
    }

    public void setPartNum(int partNum) {
        this.partNum = partNum;
    }

    public long getStartByte() {
        return startByte;
    }

    public void setStartByte(long startByte) {
        this.startByte = startByte;
    }

    public long getEndByte() {
        return endByte;
    }

    public void setEndByte(long endByte) {
        this.endByte = endByte;
    }

    public String getPartHash() {
        return partHash;
    }

    public void setPartHash(String partHash) {
        this.partHash = partHash;
    }

    public Timestamp getCompletionDate() {
        return completionDate;
    }

    public void setCompletionDate(Timestamp completionDate) {
        this.completionDate = completionDate;
    }
}
