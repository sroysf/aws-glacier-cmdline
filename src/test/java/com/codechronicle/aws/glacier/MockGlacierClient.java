package com.codechronicle.aws.glacier;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/7/12
 * Time: 5:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class MockGlacierClient implements AmazonGlacier {

    public static final String UPLOAD_ID = "testJobId";
    private String filePath;
    private int numPartInvocations = 0;
    private int numFullFileInvocations = 0;
    private int numCompleteInvocations = 0;


    public MockGlacierClient(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void setEndpoint(String endpoint) throws IllegalArgumentException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ListVaultsResult listVaults(ListVaultsRequest listVaultsRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DescribeJobResult describeJob(DescribeJobRequest describeJobRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ListPartsResult listParts(ListPartsRequest listPartsRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GetVaultNotificationsResult getVaultNotifications(GetVaultNotificationsRequest getVaultNotificationsRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ListJobsResult listJobs(ListJobsRequest listJobsRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CreateVaultResult createVault(CreateVaultRequest createVaultRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest initiateMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {

        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult().withUploadId(UPLOAD_ID);
        return result;
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deleteArchive(DeleteArchiveRequest deleteArchiveRequest) throws AmazonServiceException, AmazonClientException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GetJobOutputResult getJobOutput(GetJobOutputRequest getJobOutputRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InitiateJobResult initiateJob(InitiateJobRequest initiateJobRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public UploadArchiveResult uploadArchive(UploadArchiveRequest uploadArchiveRequest) throws AmazonServiceException, AmazonClientException {
        numFullFileInvocations++;

        UploadArchiveResult result = new UploadArchiveResult();

        File srcFile = new File(filePath);
        long archiveSize = srcFile.length();
        String fullFileChecksum = TreeHashGenerator.calculateTreeHash(srcFile);

        if (!(fullFileChecksum.equals(uploadArchiveRequest.getChecksum()))) {
            throw new AmazonClientException("Mismatched checksums, passed in : " + uploadArchiveRequest.getChecksum());
        }

        if (uploadArchiveRequest.getContentLength() != srcFile.length()) {
            throw new AmazonClientException("Mismatched content length : " + uploadArchiveRequest.getContentLength());
        }
        result.setArchiveId("mockAWSGlacierArchiveId");
        result.setChecksum(fullFileChecksum);

        return result;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setVaultNotifications(SetVaultNotificationsRequest setVaultNotificationsRequest) throws AmazonServiceException, AmazonClientException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {

        numCompleteInvocations++;

        File srcFile = new File(filePath);
        long archiveSize = srcFile.length();
        String fullFileChecksum = TreeHashGenerator.calculateTreeHash(srcFile);

        if (!(fullFileChecksum.equals(completeMultipartUploadRequest.getChecksum()))) {
            throw new AmazonClientException("Mismatched checksums, passed in : " + completeMultipartUploadRequest.getChecksum());
        }

        if (!completeMultipartUploadRequest.getArchiveSize().equals(""+srcFile.length())) {
            throw new AmazonClientException("Mismatched total archive size : " + completeMultipartUploadRequest.getArchiveSize());
        }

        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
        result.setChecksum(fullFileChecksum);
        result.setArchiveId("mockAWSGlacierArchiveId");

        return result;
    }

    @Override
    public UploadMultipartPartResult uploadMultipartPart(UploadMultipartPartRequest uploadMultipartPartRequest) throws AmazonServiceException, AmazonClientException {

        numPartInvocations++;

        String checksum = TreeHashGenerator.calculateTreeHash(uploadMultipartPartRequest.getBody());

        if (!checksum.equals(uploadMultipartPartRequest.getChecksum())) {
            throw new AmazonServiceException("Checksums do not match");
        }

        if (!UPLOAD_ID.equals(uploadMultipartPartRequest.getUploadId())) {
            throw new AmazonServiceException("Unrecognized upload id = " + uploadMultipartPartRequest.getUploadId());
        }

        UploadMultipartPartResult result = new UploadMultipartPartResult().withChecksum(checksum);
        return result;
    }

    @Override
    public DescribeVaultResult describeVault(DescribeVaultRequest describeVaultRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deleteVaultNotifications(DeleteVaultNotificationsRequest deleteVaultNotificationsRequest) throws AmazonServiceException, AmazonClientException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ListMultipartUploadsResult listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deleteVault(DeleteVaultRequest deleteVaultRequest) throws AmazonServiceException, AmazonClientException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void shutdown() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int getNumPartInvocations() {
        return numPartInvocations;
    }

    public int getNumFullFileInvocations() {
        return numFullFileInvocations;
    }

    public int getNumCompleteInvocations() {
        return numCompleteInvocations;
    }
}
