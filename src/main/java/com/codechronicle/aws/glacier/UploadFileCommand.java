package com.codechronicle.aws.glacier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.FileEntity;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/6/12
 * Time: 4:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class UploadFileCommand extends GlacierCommand implements FilePartOperator {

    private static final int ONE_MEGABYTE = 1024*1024;
    private static final int PART_SIZE = ONE_MEGABYTE * 16;

    private String filePath;
    private String vaultName;
    private String description;
    private String uploadId;

    public UploadFileCommand(Properties awsProperties, AmazonGlacier client) {
        super(awsProperties,client);
    }

    @Override
    public void executeFilePartOperation(FilePart filePart) throws FilePartException {
        ByteArrayInputStream instream = null;

        try {
            instream = new ByteArrayInputStream(filePart.getBuffer(), 0, filePart.getNumBytes());

            String treeHash = TreeHashGenerator.calculateTreeHash(instream);
            instream.reset();

            long startByte = filePart.getPartNum() * PART_SIZE;
            long endByte= startByte + filePart.getNumBytes();

            UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
                    .withAccountId(getAccountId())
                    .withBody(instream)
                    .withChecksum(treeHash)
                    .withRange("Content-Range:bytes " + startByte + "-" + endByte + "/*")
                    .withUploadId(uploadId)
                    .withVaultName(vaultName);

            System.out.println("Uploading part number : " + filePart.getPartNum() + " --> " + partRequest.getRange());
            UploadMultipartPartResult result = getClient().uploadMultipartPart(partRequest);

            if (!result.getChecksum().equals(treeHash)) {
                throw new FilePartException("Checksum mismatch. Client calculated : " + treeHash + " but AWS computed : " + result.getChecksum(), filePart);
            }
        } finally {
            IOUtils.closeQuietly(instream);
        }

    }

    @Override
    public void executeFullFileOperation(FilePart filePart) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void setVaultName(String vaultName) {
        this.vaultName = vaultName;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public void execute() {

        AmazonGlacier client = getClient();

        InitiateMultipartUploadRequest uploadJobRequest = new InitiateMultipartUploadRequest(vaultName, description, ""+PART_SIZE);
        client.initiateMultipartUpload(uploadJobRequest);

        //FileOperationSplitter fos = new FileOperationSplitter(jobId, new File(filePath), PART_SIZE);
        //fos.setFpOperator(this);
    }
}
