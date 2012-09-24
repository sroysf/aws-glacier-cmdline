package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.codechronicle.aws.glacier.command.PersistentUploadFileCommand;
import com.codechronicle.aws.glacier.command.UploadFileCommand;
import com.codechronicle.aws.glacier.dao.FileUploadPartDAO;
import com.codechronicle.aws.glacier.dbutil.HSQLDBUtil;
import com.codechronicle.aws.glacier.fileutil.FilePartException;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;


public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        AmazonGlacier client = AmazonGlacierClientFactory.getClient(awsProps);

        ComboPooledDataSource dataSource = HSQLDBUtil.initializeDatabase(null);

        try {

            testFileUpload(awsProps, client, dataSource);

            /*FileUploadPartDAO fupDAO = new FileUploadPartDAO(dataSource);
            int max = fupDAO.findMaxSuccessfulPartNumber(0);
            System.out.println("Max = " + max);*/

            Thread.sleep(10000);

        } catch (Exception ex) {
            log.error("Unexpected exception", ex);
        } finally {
            HSQLDBUtil.shutdownDatabase(dataSource);
            dataSource.close();
        }
    }

    private static void testFileUpload(Properties awsProps, AmazonGlacier client, ComboPooledDataSource dataSource) {
        PersistentUploadFileCommand cmd = new PersistentUploadFileCommand(awsProps, client, dataSource);
        cmd.setFilePath("/home/sroy/Downloads/google-chrome-stable_current_amd64.deb");
        cmd.setVault("PersonalMedia");
        cmd.execute();

        System.out.println("Result = " + cmd.getResult().getResultCode() + " [" + cmd.getResult().getMessage() + "]");
    }
}
