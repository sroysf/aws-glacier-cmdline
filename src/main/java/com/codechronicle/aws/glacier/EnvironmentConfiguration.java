package com.codechronicle.aws.glacier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/24/12
 * Time: 3:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class EnvironmentConfiguration {
    private Properties awsProperties;
    private AmazonGlacier client;
    private AWSCredentials credentials;
    private DataSource dataSource;

    public Properties getAwsProperties() {
        return awsProperties;
    }

    public void setAwsProperties(Properties awsProperties) {
        this.awsProperties = awsProperties;

        BasicAWSCredentials creds = new BasicAWSCredentials(awsProperties.getProperty("accessKey"), awsProperties.getProperty("secretKey"));
        this.credentials = creds;
    }

    public AmazonGlacier getClient() {
        return client;
    }

    public void setClient(AmazonGlacier client) {
        this.client = client;
    }

    public AWSCredentials getCredentials() {
        return credentials;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getAccountId() {
        return awsProperties.getProperty("accountId");
    }
}
