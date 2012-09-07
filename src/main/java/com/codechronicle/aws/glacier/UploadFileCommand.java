package com.codechronicle.aws.glacier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DescribeVaultOutput;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.ListVaultsRequest;
import com.amazonaws.services.glacier.model.ListVaultsResult;

import java.io.File;
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

    public UploadFileCommand(Properties awsProperties) {
        super(awsProperties);
    }

    @Override
    public void executeFilePartOperation(FilePart filePart) throws FilePartException {
        //To change body of implemented methods use File | Settings | File Templates.
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

    //@Override
    public void executeX() {

        AmazonGlacierClient client = getClient();

        // Get jobId from API call
        InitiateMultipartUploadRequest uploadJobRequest = new InitiateMultipartUploadRequest(vaultName, description, ""+PART_SIZE);

        String jobId = "AFDH23234FDD3323";

        FileOperationSplitter fos = new FileOperationSplitter(jobId, new File(filePath), PART_SIZE);
        fos.setFpOperator(this);
    }

    @Override
    public void execute() {
        AmazonGlacierClient client = getClient();
        ListVaultsRequest lvr = new ListVaultsRequest(getAccountId());
        ListVaultsResult result = client.listVaults(lvr);

        List<DescribeVaultOutput> vaultList = result.getVaultList();
        for (DescribeVaultOutput vaultDesc : vaultList) {
            System.out.println(vaultDesc.getVaultName());
        }
    }
}
