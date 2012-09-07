package com.codechronicle.aws.glacier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/5/12
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class GlacierCommand {

    private Properties awsProperties;
    private AWSCredentials credentials;

    protected GlacierCommand(Properties awsProperties) {
        this.awsProperties = awsProperties;
        credentials = new BasicAWSCredentials(awsProperties.getProperty("accessKey"), awsProperties.getProperty("secretKey"));
    }

    protected AWSCredentials getCredentials() {
        return credentials;
    }

    protected AmazonGlacierClient getClient() {
        AmazonGlacierClient client = new AmazonGlacierClient(getCredentials());
        client.setEndpoint(awsProperties.getProperty("endPoint"));
        return client;
    }

    protected String getAccountId() {
        return awsProperties.getProperty("accountId");
    }

    public abstract void execute();
}
