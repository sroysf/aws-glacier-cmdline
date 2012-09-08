package com.codechronicle.aws.glacier;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.model.*;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/7/12
 * Time: 5:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class MockGlacierClient implements AmazonGlacier {
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

        System.out.println("MOCK : " + initiateMultipartUploadRequest.getArchiveDescription());

        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult().withUploadId("testJobId");
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setVaultNotifications(SetVaultNotificationsRequest setVaultNotificationsRequest) throws AmazonServiceException, AmazonClientException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public UploadMultipartPartResult uploadMultipartPart(UploadMultipartPartRequest uploadMultipartPartRequest) throws AmazonServiceException, AmazonClientException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
}
