package com.codechronicle.aws.glacier.command;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.mchange.v2.c3p0.PooledDataSource;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/22/12
 * Time: 3:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class PersistentUploadFileCommand extends GlacierCommand {

    public PersistentUploadFileCommand(Properties awsProperties, AmazonGlacier client, PooledDataSource dataSource) {
        super(awsProperties, client, dataSource);
    }

    @Override
    public void execute() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
