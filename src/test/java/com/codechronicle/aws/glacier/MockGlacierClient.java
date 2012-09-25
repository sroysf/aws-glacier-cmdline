package com.codechronicle.aws.glacier;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/7/12
 * Time: 5:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class MockGlacierClient implements AmazonGlacier {

    public static final String UPLOAD_ID = "testJobId";
    private File tempUploadDirectory;

    public MockGlacierClient() {
        tempUploadDirectory = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
        tempUploadDirectory.mkdirs();
        System.out.println("MockGlacierClient using upload directory : " + tempUploadDirectory.getAbsolutePath());
    }

    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(tempUploadDirectory);
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest initiateMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {

        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult().withUploadId(UPLOAD_ID);
        return result;
    }



    @Override
    public UploadArchiveResult uploadArchive(UploadArchiveRequest uploadArchiveRequest) throws AmazonServiceException, AmazonClientException {
        UploadArchiveResult result = new UploadArchiveResult();

        String vault = uploadArchiveRequest.getVaultName();
        File vaultDir = createVaultDirectory(vault);

        String archiveId = generateArchiveId();
        File storedFile = new File(vaultDir, archiveId);
        try {
            FileUtils.copyInputStreamToFile(uploadArchiveRequest.getBody(), storedFile);
        } catch (IOException e) {
            throw new AmazonServiceException("Error while storing file", e);
        }

        // Compare hashes
        String computedHash = TreeHashGenerator.calculateTreeHash(storedFile);
        if (!uploadArchiveRequest.getChecksum().equals(computedHash)) {
            throw new AmazonServiceException("Hash codes did not match");
        }

        result.setArchiveId(archiveId);
        result.setChecksum(computedHash);
        result.setLocation("/aws/fake/url/" + archiveId);

        return result;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private String generateArchiveId() {
        return UUID.randomUUID().toString();
    }

    public File getUploadDirectory() {
        return tempUploadDirectory;
    }

    private File createVaultDirectory(String vault) {
        File vaultDir = new File(tempUploadDirectory, vault);
        if (!vaultDir.exists()) {
            vaultDir.mkdirs();
        }

        return vaultDir;
    }


    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {

        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
        return result;
    }

    @Override
    public UploadMultipartPartResult uploadMultipartPart(UploadMultipartPartRequest uploadMultipartPartRequest) throws AmazonServiceException, AmazonClientException {
        UploadMultipartPartResult result = new UploadMultipartPartResult();
        return result;
    }

    // =========================
    // Unimplemented Methods

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

    @Override
    public void setVaultNotifications(SetVaultNotificationsRequest setVaultNotificationsRequest) throws AmazonServiceException, AmazonClientException {
        //To change body of implemented methods use File | Settings | File Templates.
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
}
