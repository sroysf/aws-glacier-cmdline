package com.codechronicle.aws.glacier.command;

import com.amazonaws.services.glacier.TreeHashGenerator;
import com.codechronicle.aws.glacier.AppConstants;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.MockGlacierClient;
import com.codechronicle.aws.glacier.TestFileGenerator;
import com.codechronicle.aws.glacier.dbutil.HSQLDBUtil;
import com.codechronicle.aws.glacier.event.Event;
import com.codechronicle.aws.glacier.event.EventListener;
import com.codechronicle.aws.glacier.event.EventRegistry;
import com.codechronicle.aws.glacier.event.EventType;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.mchange.v2.c3p0.PooledDataSource;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/7/12
 * Time: 4:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class PersistentUploadFileCommandTest {

    private File largeFile;
    private File exactFile;
    private File smallFile;
    private PooledDataSource datasource;
    private EnvironmentConfiguration config;
    private MockGlacierClient client = new MockGlacierClient();

    //TODO: Test all kinds of invalid inputs
    //TODO: Upgrade MockGlacierClient to actually piece together files so it's an accurate representation of what goes
    //      on within AWS


    @BeforeSuite
    public void generateTestFiles() throws IOException {

        largeFile = File.createTempFile("UploadFileCommandTest.largeInputFile", ".dat");
        smallFile = File.createTempFile("UploadFileCommandTest.smallInputFile", ".dat");
        exactFile = File.createTempFile("UploadFileCommandTest.exactInputFile", ".dat");

        TestFileGenerator.writeFileOfSize(largeFile, (int) (AppConstants.NETWORK_PARTITION_SIZE * 2.5));
        TestFileGenerator.writeFileOfSize(exactFile, (int) (AppConstants.NETWORK_PARTITION_SIZE));
        TestFileGenerator.writeFileOfSize(smallFile, (int)(AppConstants.NETWORK_PARTITION_SIZE * 0.7));

        String uuid = UUID.randomUUID().toString();
        File workingDir = new File(FileUtils.getTempDirectory(), uuid);

        System.out.println("Working directory = " + workingDir.getAbsolutePath());
        this.datasource = HSQLDBUtil.initializeDatabase(workingDir);

        config = new EnvironmentConfiguration();

        Properties awsProps = new Properties();  // Don't need real credentials for unit testing
        awsProps.setProperty("accessKey", "dummyKey");
        awsProps.setProperty("secretKey", "dummyKey");
        config.setAwsProperties(awsProps);
        config.setDataSource(this.datasource);
        config.setClient(client);
    }

    @AfterSuite
    public void cleanupTestFiles() {
        if (largeFile != null) {
            largeFile.delete();
        }

        if (smallFile != null) {
            smallFile.delete();
        }

        if (exactFile != null) {
            exactFile.delete();
        }

        HSQLDBUtil.shutdownDatabase(this.datasource);
    }

    private List<Event> events = new ArrayList<Event>();

    @Test
    public void testFileUploads() throws Exception {

        EventRegistry.clearAllListeners();
        EventRegistry.register(EventType.UPLOAD_COMPLETE, new EventListener() {
            @Override
            public void onEvent(Event event) {
                events.add(event);

                synchronized (client) {
                    client.notify();
                }
            }
        });

        uploadFile(smallFile);
        uploadFile(exactFile);
        uploadFile(largeFile);

        while (events.size() < 3) {
            synchronized (client) {
                client.wait();
                System.out.println("Received " + events.size() + " completion events");
            }
        }

        validateUploadedFiles();
    }

    private void validateUploadedFiles() {
        File vaultDir = new File(client.getUploadDirectory(), "testVault");
        File[] files = vaultDir.listFiles();
        List<String> hashes = new ArrayList<String>();
        for (File file : files) {
            hashes.add(TreeHashGenerator.calculateTreeHash(file));
        }

        assertTrue(findMatchingHash(smallFile, hashes), "Unable to find small file in expected vault dir = " + vaultDir.getAbsolutePath());
        assertTrue(findMatchingHash(largeFile, hashes), "Unable to find large file in expected vault dir = " + vaultDir.getAbsolutePath());
        assertTrue(findMatchingHash(exactFile, hashes), "Unable to find exact file in expected vault dir = " + vaultDir.getAbsolutePath());
    }

    private boolean findMatchingHash(File file, List<String> hashes) {
        String hash = TreeHashGenerator.calculateTreeHash(file);
        for (String fileHash : hashes) {
            if (hash.equals(fileHash)) {
                return true;
            }
        }

        return false;
    }

    private void uploadFile(File file) {
        PersistentUploadFileCommand cmd = new PersistentUploadFileCommand(config);
        cmd.setFilePath(file.getAbsolutePath());
        cmd.setVault("testVault");

        cmd.execute();

        CommandResult result = cmd.getResult();
        assertEquals(CommandResultCode.SUCCESS.ordinal(), result.getResultCode().ordinal());
    }

    /*@Test
    public void testUploadLargeFile () throws Exception{

        Properties awsProps = getAwsProperties();

        final MockGlacierClient client = new MockGlacierClient(largeFile.getAbsolutePath());
        PersistentUploadFileCommand cmd = new PersistentUploadFileCommand(awsProps, client, datasource);
        cmd.setFilePath(largeFile.getAbsolutePath());
        cmd.setEventListener(new FileUploadEventListener() {
            @Override
            public void onUploadComplete(FileUploadRecord record) {
                synchronized (client) {
                    client.notify();
                }
            }
        });

        cmd.execute();
        synchronized (client) {
            System.out.println("Waiting for asynchronous command to complete...");
            client.wait(10000);
        }

        try {
            int expectedInvocations = (int)(largeFile.length() / AppConstants.NETWORK_PARTITION_SIZE) + 1;
            Assert.assertEquals(1, client.getNumCompleteInvocations());
            Assert.assertEquals(0, client.getNumFullFileInvocations());
            Assert.assertEquals(expectedInvocations, client.getNumPartInvocations());
        } finally {
            client.cleanup();
        }

    }



    @Test
    public void testUploadExactFile () throws Exception{

        Properties awsProps = getAwsProperties();

        final MockGlacierClient client = new MockGlacierClient(exactFile.getAbsolutePath());
        PersistentUploadFileCommand cmd = new PersistentUploadFileCommand(awsProps, client, datasource);
        cmd.setFilePath(exactFile.getAbsolutePath());
        cmd.setEventListener(new FileUploadEventListener() {
            @Override
            public void onUploadComplete(FileUploadRecord record) {
                synchronized (client) {
                    client.notify();
                }
            }
        });

        cmd.execute();
        synchronized (client) {
            System.out.println("Waiting for asynchronous command to complete...");
            client.wait(10000);
        }

        try {
            int expectedInvocations = 1;
            Assert.assertEquals(1, client.getNumCompleteInvocations());
            Assert.assertEquals(0, client.getNumFullFileInvocations());
            Assert.assertEquals(expectedInvocations, client.getNumPartInvocations());
        } finally {
            client.cleanup();
        }
    }

    @Test
    public void testUploadSmallFile () throws Exception{

        Properties awsProps = getAwsProperties();

        final MockGlacierClient client = new MockGlacierClient(smallFile.getAbsolutePath());
        PersistentUploadFileCommand cmd = new PersistentUploadFileCommand(awsProps, client, datasource);
        cmd.setFilePath(smallFile.getAbsolutePath());
        cmd.setEventListener(new FileUploadEventListener() {
            @Override
            public void onUploadComplete(FileUploadRecord record) {
                synchronized (client) {
                    client.notify();
                }
            }
        });

        cmd.execute();
        synchronized (client) {
            System.out.println("Waiting for asynchronous command to complete...");
            client.wait(10000);
        }

        try {
            Assert.assertEquals(0, client.getNumCompleteInvocations());
            Assert.assertEquals(0, client.getNumPartInvocations());
            Assert.assertEquals(1, client.getNumFullFileInvocations());
        } finally {
            client.cleanup();;
        }
    }*/
}
