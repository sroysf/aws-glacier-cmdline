package com.codechronicle.aws.glacier.command;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
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
    private AmazonGlacier client;
    private AWSCredentials credentials;

    protected GlacierCommand(Properties awsProperties, AmazonGlacier client) {
        this.awsProperties = awsProperties;
        this.client = client;
    }

    protected AWSCredentials getCredentials() {
        return credentials;
    }

    protected AmazonGlacier getClient() {
        /*
        AmazonGlacierClient client = new AmazonGlacierClient(getCredentials());
        client.setEndpoint(awsProperties.getProperty("endPoint"));
        */
        return client;
    }

    protected String getAccountId() {
        return awsProperties.getProperty("accountId");
    }

    public abstract void execute();
}
