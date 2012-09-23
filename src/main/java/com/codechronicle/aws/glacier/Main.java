package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.codechronicle.aws.glacier.command.PersistentUploadFileCommand;
import com.codechronicle.aws.glacier.command.UploadFileCommand;
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
            QueryRunner insertQR = new QueryRunner(dataSource);
            insertQR.update("INSERT INTO PUBLIC.COMMAND (action,params) VALUES (?,?)",
                    "Upload", "{json -" + new java.util.Date().toString() + "}");

            QueryRunner qr = new QueryRunner(dataSource);
            qr.query("SELECT * FROM PUBLIC.COMMAND", new ResultSetHandler<Object>() {
                @Override
                public Object handle(ResultSet rs) throws SQLException {
                    while (rs.next()) {
                        System.out.println(rs.getString("action") + " ==> " + rs.getString("params"));
                    }
                    return null;
                }
            });

        } catch (Exception ex) {
            log.error("Unexpected exception", ex);
        } finally {
            HSQLDBUtil.shutdownDatabase(dataSource);
            dataSource.close();
        }
    }

    public static void mainX(String[] args) throws IOException, FilePartException {

        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        AmazonGlacier client = AmazonGlacierClientFactory.getClient(awsProps);

        UploadFileCommand cmd = new UploadFileCommand(awsProps, client);
        //cmd.setFilePath("/home/saptarshi.roy/Downloads/blackduck-bdspest-linux.bin");
        cmd.setFilePath("/Users/saptarshi.roy/Downloads/ubuntu-12.04.1-desktop-amd64.iso");
        cmd.setDescription("blackduck-bdspest-linux.bin");
        cmd.setVaultName("PersonalMedia");

        cmd.execute();
    }
}
