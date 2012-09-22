package com.codechronicle.aws.glacier.command;

import com.codechronicle.aws.glacier.MockGlacierClient;
import com.codechronicle.aws.glacier.TestFileGenerator;
import com.codechronicle.aws.glacier.command.UploadFileCommand;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/7/12
 * Time: 4:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class UploadFileCommandTest {

    private File largeFile;
    private File smallFile;

    //TODO: Test all kinds of invalid inputs


    @BeforeSuite
    public void generateTestFiles() throws IOException {

        largeFile = File.createTempFile("UploadFileCommandTest.largeInputFile", ".dat");
        smallFile = File.createTempFile("UploadFileCommandTest.smallInputFile", ".dat");

        TestFileGenerator.writeFileOfSize(largeFile, (int) (UploadFileCommand.PART_SIZE * 2.5));
        TestFileGenerator.writeFileOfSize(smallFile, (int)(UploadFileCommand.PART_SIZE * 0.7));
    }

    @AfterSuite
    public void cleanupTestFiles() {
        if (largeFile != null) {
            largeFile.delete();
        }

        if (smallFile != null) {
            smallFile.delete();
        }
    }

    @Test
    public void testUploadLargeFile () throws Exception{

        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        MockGlacierClient client = new MockGlacierClient(largeFile.getAbsolutePath());
        UploadFileCommand cmd = new UploadFileCommand(awsProps, client);
        cmd.setFilePath(largeFile.getAbsolutePath());
        cmd.setDescription("Test file upload");

        cmd.execute();

        int expectedInvocations = (int)(largeFile.length() / UploadFileCommand.PART_SIZE) + 1;
        Assert.assertEquals(1, client.getNumCompleteInvocations());
        Assert.assertEquals(0, client.getNumFullFileInvocations());
        Assert.assertEquals(expectedInvocations, client.getNumPartInvocations());
    }

    @Test
    public void testUploadSmallFile () throws Exception{

        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        MockGlacierClient client = new MockGlacierClient(smallFile.getAbsolutePath());
        UploadFileCommand cmd = new UploadFileCommand(awsProps, client);
        cmd.setFilePath(smallFile.getAbsolutePath());
        cmd.setDescription("Test file upload");

        cmd.execute();

        Assert.assertEquals(0, client.getNumCompleteInvocations());
        Assert.assertEquals(1, client.getNumFullFileInvocations());
        Assert.assertEquals(0, client.getNumPartInvocations());
    }
}
