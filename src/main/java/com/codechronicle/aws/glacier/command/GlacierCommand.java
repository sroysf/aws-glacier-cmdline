package com.codechronicle.aws.glacier.command;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.mchange.v2.c3p0.PooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;
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
    private PooledDataSource dataSource;

    protected GlacierCommand(Properties awsProperties, AmazonGlacier client, PooledDataSource dataSource) {
        this.awsProperties = awsProperties;
        this.client = client;
        this.dataSource = dataSource;
    }

    protected AWSCredentials getCredentials() {
        return credentials;
    }

    protected AmazonGlacier getClient() {
        return client;
    }

    protected String getAccountId() {
        return awsProperties.getProperty("accountId");
    }

    protected Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public abstract void execute();
}
