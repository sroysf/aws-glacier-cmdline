package com.codechronicle.aws.glacier.command;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
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

    private EnvironmentConfiguration config;
    private CommandResult result;

    private static Logger log = LoggerFactory.getLogger(GlacierCommand.class);

    protected GlacierCommand(EnvironmentConfiguration config) {
        this.config = config;
        this.result = new CommandResult();
    }

    public EnvironmentConfiguration getConfig() {
        return config;
    }

    public CommandResult getResult() {
        return result;
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
