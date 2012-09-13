package com.codechronicle.aws.glacier;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/13/12
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class AmazonGlacierClientFactory {
    public static AmazonGlacier getClient(Properties awsProperties) {

        BasicAWSCredentials credentials = new BasicAWSCredentials(awsProperties.getProperty("accessKey"), awsProperties.getProperty("secretKey"));
        AmazonGlacier client = new AmazonGlacierClient(credentials);
        client.setEndpoint(awsProperties.getProperty("endPoint"));

        return client;
    }
}
