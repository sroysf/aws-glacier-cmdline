package com.codechronicle.aws.glacier.localtest;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.cmdline.CommandLineProcessor;
import com.codechronicle.aws.glacier.dbutil.HSQLDBUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * Use this class to exercise the system locally, without actually calling out to AWS.
 * This uses an implementation of the GlacierClient to simulate the functionality of the AWS endpoint.
 *
 * Uploaded files and the local store database are placed in ~/glacierTestingDir
 *
 */
public class MainTestDriver {

    private static Logger log = LoggerFactory.getLogger(MainTestDriver.class);

    public static void main(String[] args) {

        Properties awsProps = new Properties();
        awsProps.put("accessKey", "mockAccesskey");
        awsProps.put("secretKey", "mockSecretKey");

        File workingDir = new File(System.getenv("HOME") + "/glacierTestingDir");
        workingDir.mkdirs();

        File mockAWSDir = new File(workingDir, "mockAWSStore");
        File dbDir = new File(workingDir, "localdatastore");

        final MockGlacierClient client = new MockGlacierClient(mockAWSDir);
        ComboPooledDataSource dataSource = HSQLDBUtil.initializeDatabase(dbDir);
        client.setUploadTimePerPart(10);

        EnvironmentConfiguration config = new EnvironmentConfiguration();
        config.setAwsProperties(awsProps);
        config.setClient(client);
        config.setDataSource(dataSource);

        try {

            CommandLineProcessor clp = new CommandLineProcessor(config);
            clp.startProcessingUserInput();

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            log.info("Cleaning up resources...");
            dataSource.setAutoCommitOnClose(true);
            dataSource.close();
        }

        log.info("Exiting...");
    }
}
