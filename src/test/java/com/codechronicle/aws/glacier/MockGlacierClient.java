package com.codechronicle.aws.glacier;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;

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

    private Map<String,InitiateMultipartUploadRequest> multiPartMap = new HashMap<String, InitiateMultipartUploadRequest>();
    private Map<String,List<UploadMultipartPartRequest>> inProgressPartsMap = new HashMap<String, List<UploadMultipartPartRequest>>();

    public MockGlacierClient() {
        tempUploadDirectory = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
        tempUploadDirectory.mkdirs();
        System.out.println("MockGlacierClient using upload directory : " + tempUploadDirectory.getAbsolutePath());
    }

    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(tempUploadDirectory);
    }


    @Override
    public UploadArchiveResult uploadArchive(UploadArchiveRequest uploadArchiveRequest) throws AmazonServiceException, AmazonClientException {
        UploadArchiveResult result = new UploadArchiveResult();

        String vault = uploadArchiveRequest.getVaultName();
        File vaultDir = createVaultDirectory(vault);

        String archiveId = generateArchiveId();
        File storedFile = new File(vaultDir, archiveId);
        try {
            // Test streams, based on real-world experience with Amazon
            InputStream instream = uploadArchiveRequest.getBody();
            if (!instream.markSupported()) {
                throw new AmazonClientException("Input stream must support mark and reset properly");
            }
            instream.mark(10);
            instream.read();
            instream.read();
            instream.reset();

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
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest initiateMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {
        String uploadId = "AWS-ID-" + UUID.randomUUID().toString();

        //TODO: Add validation of partition size

        multiPartMap.put(uploadId, initiateMultipartUploadRequest);
        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult().withUploadId(uploadId);
        return result;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) throws AmazonServiceException, AmazonClientException {

        final String uploadId = completeMultipartUploadRequest.getUploadId();
        InitiateMultipartUploadRequest request = multiPartMap.get(uploadId);

        if (request == null) {
            throw new AmazonClientException("Invalid upload id = " + uploadId);
        }

        File vaultDir = createVaultDirectory(request.getVaultName());
        File[] parts = vaultDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(uploadId);  //To change body of implemented methods use File | Settings | File Templates.
            }
        });

        List<File> partsList = new ArrayList<File>();
        for (File part : parts) {
            partsList.add(part);
        }
        Collections.sort(partsList);

        // Stitch all the parts together
        System.out.println("Stitching part files together...");
        String archiveId = generateArchiveId();
        File combinedFile = null;

        try {
            combinedFile = stitchParts(vaultDir, archiveId, partsList);
        } catch (IOException e) {
            throw new AmazonServiceException("Unable to stitch files together : " + vaultDir.getAbsolutePath() + "/" + archiveId);
        }

        // Calculate and compare hash
        String fullHash = TreeHashGenerator.calculateTreeHash(combinedFile);
        if (!completeMultipartUploadRequest.getChecksum().equals(fullHash)) {
            throw new AmazonClientException("Hashes for full file did not match : " + completeMultipartUploadRequest.getChecksum() + "==" + fullHash);
        }

        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
        result.setChecksum(fullHash);
        result.setArchiveId(archiveId);

        return result;
    }

    private File stitchParts(File vaultDir, String archiveId, List<File> partsList) throws IOException {

        File outfile = new File(vaultDir, archiveId);
        BufferedOutputStream outputStream = null;

        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(outfile, true));
            for (File partFile : partsList) {
                InputStream instream = null;

                try {
                    instream = FileUtils.openInputStream(partFile);
                    IOUtils.copy(instream, outputStream);
                } finally {
                    IOUtils.closeQuietly(instream);
                }
            }
        } finally {
            if (outputStream != null) {
                IOUtils.closeQuietly(outputStream);
            }
        }
        return outfile;
    }


    @Override
    public UploadMultipartPartResult uploadMultipartPart(UploadMultipartPartRequest uploadMultipartPartRequest) throws AmazonServiceException, AmazonClientException {

        InitiateMultipartUploadRequest request = multiPartMap.get(uploadMultipartPartRequest.getUploadId());
        if (request == null) {
            throw new AmazonClientException("No matching pending multipart request found for upload id = " + uploadMultipartPartRequest.getUploadId());
        }

        File vaultDir = createVaultDirectory(request.getVaultName());
        int numParts = addPartTracker(uploadMultipartPartRequest.getUploadId(), uploadMultipartPartRequest);

        File partFile = new File(vaultDir, uploadMultipartPartRequest.getUploadId() + ".part." + numParts);
        try {
            FileUtils.copyInputStreamToFile(uploadMultipartPartRequest.getBody(), partFile);
        } catch (IOException e) {
            throw new AmazonServiceException("While writing file", e);
        }

        UploadMultipartPartResult result = new UploadMultipartPartResult();

        String checksum = TreeHashGenerator.calculateTreeHash(partFile);

        if (!checksum.equals(uploadMultipartPartRequest.getChecksum())) {
            throw new AmazonClientException("Hashes did not match for uploaded part : " +  uploadMultipartPartRequest.getUploadId() + ":" + uploadMultipartPartRequest.getRange());
        }

        result.setChecksum(checksum);

        return result;
    }

    private int addPartTracker(String uploadId, UploadMultipartPartRequest uploadMultipartPartRequest) {
        List<UploadMultipartPartRequest> parts = inProgressPartsMap.get(uploadId);
        if (parts == null) {
            parts = new ArrayList<UploadMultipartPartRequest>();
            inProgressPartsMap.put(uploadId, parts);
        }

        if (findByRange(parts, uploadMultipartPartRequest) == null) {
            parts.add(uploadMultipartPartRequest);
        }

        return parts.size();
    }

    private UploadMultipartPartRequest findByRange(List<UploadMultipartPartRequest> parts, UploadMultipartPartRequest uploadMultipartPartRequest) {
        for (UploadMultipartPartRequest part : parts) {
            if (part.getRange().equals(uploadMultipartPartRequest.getRange())) {
                return part;
            }
        }

        return null;
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
