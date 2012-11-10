package com.codechronicle.aws.glacier;


import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.cmdline.CommandLineProcessor;
import com.codechronicle.aws.glacier.dbutil.HSQLDBUtil;
import com.codechronicle.aws.glacier.localtest.MockGlacierClient;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * Requires ~/.aws/aws.properties
 *
 * AWS Properties file contents:
 * accountId=
 * canonicalUserId=
 * endPoint=glacier.us-west-2.amazonaws.com
 * accessKey=
 * secretKey=
 *
 */
public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        // Use default behavior of HSQLDBUtil
        ComboPooledDataSource dataSource = HSQLDBUtil.initializeDatabase(null);

        try {
            Properties awsProps = loadAWSPropertiesFromDefaultFile();

            dataSource.setAutoCommitOnClose(true);

            AmazonGlacier client = AmazonGlacierClientFactory.getClient(awsProps);

            EnvironmentConfiguration config = new EnvironmentConfiguration();
            config.setAwsProperties(awsProps);
            config.setClient(client);
            config.setDataSource(dataSource);

            CommandLineProcessor clp = new CommandLineProcessor(config);
            clp.startProcessingUserInput();

        } catch (Exception e) {
            log.error("Unexpected error : " + e);
            System.out.println(e.getMessage());
        } finally {
            HSQLDBUtil.shutdownDatabase(dataSource);
            dataSource.close();
        }

    }

    /*public static void mainX(String[] args) {

        ComboPooledDataSource dataSource = HSQLDBUtil.initializeDatabase(null);
        try {

            Properties awsProps = loadAWSPropertiesFromDefaultFile();

            dataSource.setAutoCommitOnClose(true);

            client.setUploadTimePerPart(5);

            EnvironmentConfiguration config = new EnvironmentConfiguration();
            config.setAwsProperties(awsProps);
            config.setClient(client);
            config.setDataSource(dataSource);

            CommandLineProcessor clp = new CommandLineProcessor(config);
            clp.startProcessingUserInput();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            HSQLDBUtil.shutdownDatabase(dataSource);
            dataSource.close();
        }

        log.info("Exiting...");
    }*/

    private static Properties loadAWSPropertiesFromDefaultFile() throws FileNotFoundException {
        File propsFile = new File(new File(FileUtils.getUserDirectory(), ".aws"), "aws.properties");
        System.out.println(propsFile.getAbsolutePath());

        if (!(propsFile.exists() && propsFile.canRead())) {
            throw new FileNotFoundException("Unable to read Amazon Web Services configuration from : " + propsFile.getAbsolutePath());
        }

        Properties props = new Properties();
        FileInputStream fis = null;
        try {
            fis = FileUtils.openInputStream(propsFile);
            props.load(fis);
        } catch (IOException e) {
            log.error("Unable to read file", e);
        } finally {
            IOUtils.closeQuietly(fis);
        }

        return props;
    }
}
