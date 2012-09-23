package com.codechronicle.aws.glacier.command;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.mchange.v2.c3p0.PooledDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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
    private CommandResult result;
    private DataSource dataSource;

    private static Logger log = LoggerFactory.getLogger(GlacierCommand.class);

    protected GlacierCommand(Properties awsProperties, AmazonGlacier client, DataSource dataSource) {
        this.awsProperties = awsProperties;
        this.client = client;
        this.result = new CommandResult();
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

    public CommandResult getResult() {
        return result;
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    public void execute() {
        try {
            executeCustomLogic();
        } catch (Exception ex) {
            this.result.setMessage("Unexpected Error");
            this.result.setResultCode(CommandResultCode.UNEXPECTED_ERROR);
            log.error("Unexpected error", ex);
        }
    }

    protected abstract void executeCustomLogic() throws Exception;
}
