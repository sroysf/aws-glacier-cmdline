package com.codechronicle.aws.glacier;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.FileEntity;

import java.io.*;
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
    public static final int PART_SIZE = ONE_MEGABYTE * 16;

    private String filePath;
    private String vaultName;
    private String description;
    private String uploadId;
    private String archiveId;

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
        BufferedInputStream instream = null;
        try {
            instream = new BufferedInputStream(new FileInputStream(filePart.getFile()));
            String checksum = TreeHashGenerator.calculateTreeHash(filePart.getFile());
            long contentLength = filePart.getFile().length();

            UploadArchiveRequest request = new UploadArchiveRequest()
                    .withAccountId(getAccountId())
                    .withArchiveDescription(description)
                    .withBody(instream)
                    .withChecksum(checksum)
                    .withContentLength(contentLength)
                    .withVaultName(vaultName);

            UploadArchiveResult result = getClient().uploadArchive(request);
            if (!result.getChecksum().equals(checksum)) {
                throw new AmazonServiceException("Checksum mismatch. Client calculated : " + checksum + " but AWS computed : " + result.getChecksum());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            IOUtils.closeQuietly(instream);
        }
    }

    @Override
    public void close() {

        File srcFile = new File(filePath);

        if (srcFile.length() <= PART_SIZE) {
            // This is only needed in case of multi-part upload.
            return;
        }


        long archiveSize = srcFile.length();
        String fullFileChecksum = TreeHashGenerator.calculateTreeHash(srcFile);

        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest();
        completeRequest.setAccountId(getAccountId());
        completeRequest.setVaultName(vaultName);
        completeRequest.setArchiveSize("" + archiveSize);
        completeRequest.setChecksum(fullFileChecksum);
        completeRequest.setUploadId(uploadId);
        completeRequest.setRequestCredentials(getCredentials());

        CompleteMultipartUploadResult result = getClient().completeMultipartUpload(completeRequest);
        String awsChecksum = result.getChecksum();
        if (!fullFileChecksum.equals(awsChecksum)) {
            throw new RuntimeException("AWS checksum for full file did not match calculated checksum. AWS=" + awsChecksum + " Calculated=" + fullFileChecksum);
        }

        this.archiveId = result.getArchiveId();
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
        InitiateMultipartUploadResult result = client.initiateMultipartUpload(uploadJobRequest);
        this.uploadId = result.getUploadId();

        FileOperationSplitter fos = new FileOperationSplitter(this.uploadId, new File(filePath), PART_SIZE);
        fos.setFpOperator(this);

        try {
            fos.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
