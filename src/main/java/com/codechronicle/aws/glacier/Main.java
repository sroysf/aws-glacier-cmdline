package com.codechronicle.aws.glacier;

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

        UploadFileCommand cmd = new UploadFileCommand(awsProps);
        cmd.setFilePath("/home/saptarshi.roy/Downloads/ubuntu-10.04.4-server-amd64.iso");
        cmd.execute();
    }
}
