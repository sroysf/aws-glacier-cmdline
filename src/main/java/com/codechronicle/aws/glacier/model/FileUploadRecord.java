package com.codechronicle.aws.glacier.model;

import com.codechronicle.aws.glacier.command.CommandResultCode;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 12:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUploadRecord {
    private int id;
    private String awsUploadId;
    private String fileName;
    private String fileHash;
    private String vault;
    private String json;
    private FileUploadStatus status;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAwsUploadId() {
        return awsUploadId;
    }

    public void setAwsUploadId(String awsUploadId) {
        this.awsUploadId = awsUploadId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileHash() {
        return fileHash;
    }

    public void setFileHash(String fileHash) {
        this.fileHash = fileHash;
    }

    public String getVault() {
        return vault;
    }

    public void setVault(String vault) {
        this.vault = vault;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public FileUploadStatus getStatus() {
        return status;
    }

    public void setStatus(FileUploadStatus status) {
        this.status = status;
    }
}
