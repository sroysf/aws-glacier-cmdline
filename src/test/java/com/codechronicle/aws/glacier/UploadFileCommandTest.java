package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.AmazonGlacier;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/7/12
 * Time: 4:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class UploadFileCommandTest {

    @Test
    public void testUploadFile () throws Exception{

        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        AmazonGlacier client = new MockGlacierClient();
        UploadFileCommand cmd = new UploadFileCommand(awsProps, client);
        //cmd.setFilePath("/home/saptarshi.roy/Downloads/ubuntu-10.04.4-server-amd64.iso");
        cmd.setFilePath("/home/sroy/Downloads/eclipse-indigo.tar.gz");
        cmd.setDescription("Test file upload");

        cmd.execute();

    }
}
