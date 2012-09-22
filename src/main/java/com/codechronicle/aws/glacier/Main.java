package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.codechronicle.aws.glacier.command.UploadFileCommand;
import com.codechronicle.aws.glacier.fileutil.FilePartException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;


public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, FilePartException {

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
