package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.codechronicle.aws.glacier.command.PersistentUploadFileCommand;
import com.codechronicle.aws.glacier.dbutil.HSQLDBUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;


public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        String[] files = new String[] {"/tmp/fb901e8b-ffcf-4d13-b0c7-d56c8dbe3984/testVault/AWS-ID-b603ce56-a3d0-4299-b8e2-e045f3bae8e7.part.1",
        "/tmp/fb901e8b-ffcf-4d13-b0c7-d56c8dbe3984/testVault/AWS-ID-b603ce56-a3d0-4299-b8e2-e045f3bae8e7.part.2",
        "/tmp/fb901e8b-ffcf-4d13-b0c7-d56c8dbe3984/testVault/AWS-ID-b603ce56-a3d0-4299-b8e2-e045f3bae8e7.part.3"};

        File outfile = new File("/tmp/fb901e8b-ffcf-4d13-b0c7-d56c8dbe3984/testVault/combined");
        BufferedOutputStream outputStream = null;

        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(outfile, true));
            for (String file : files) {
                File partFile = new File(file);
                InputStream instream = null;

                try {
                    instream = FileUtils.openInputStream(partFile);
                    IOUtils.copy(instream, outputStream);
                } finally {
                    IOUtils.closeQuietly(instream);
                }
            }
        } finally {
            if (outputStream != null) {
                IOUtils.closeQuietly(outputStream);
            }
        }
    }

    public static void mainX(String[] args) throws IOException {

        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        AmazonGlacier client = AmazonGlacierClientFactory.getClient(awsProps);

        ComboPooledDataSource dataSource = HSQLDBUtil.initializeDatabase(null);

        try {

            testFileUpload(awsProps, client, dataSource);

            Thread.sleep(10000);

        } catch (Exception ex) {
            log.error("Unexpected exception", ex);
        } finally {
            HSQLDBUtil.shutdownDatabase(dataSource);
            dataSource.close();
        }
    }

    private static void testFileUpload(Properties awsProps, AmazonGlacier client, ComboPooledDataSource dataSource) {
        /*PersistentUploadFileCommand cmd = new PersistentUploadFileCommand(awsProps, client, dataSource);
        cmd.setFilePath("/home/sroy/Downloads/google-chrome-stable_current_amd64.deb");
        cmd.setVault("PersonalMedia");
        cmd.execute();

        System.out.println("Result = " + cmd.getResult().getResultCode() + " [" + cmd.getResult().getMessage() + "]");*/
    }
}
