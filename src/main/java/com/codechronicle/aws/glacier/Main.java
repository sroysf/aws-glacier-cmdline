package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.codechronicle.aws.glacier.command.PersistentUploadFileCommand;
import com.codechronicle.aws.glacier.dbutil.HSQLDBUtil;
import com.codechronicle.aws.glacier.event.Event;
import com.codechronicle.aws.glacier.event.EventListener;
import com.codechronicle.aws.glacier.event.EventRegistry;
import com.codechronicle.aws.glacier.event.EventType;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;


public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws IOException {

        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        final AmazonGlacier client = AmazonGlacierClientFactory.getClient(awsProps);

        ComboPooledDataSource dataSource = HSQLDBUtil.initializeDatabase(null);

        try {

            EnvironmentConfiguration config = new EnvironmentConfiguration();
            config.setAwsProperties(awsProps);
            config.setClient(client);
            config.setDataSource(dataSource);

            EventRegistry.register(EventType.UPLOAD_COMPLETE, new EventListener() {
                @Override
                public void onEvent(Event event) {
                    FileUploadRecord record = (FileUploadRecord)event.getMessagePayload();
                    log.info("Completed upload of file : " + record.getFilePath());
                }
            });

            PersistentUploadFileCommand cmd = new PersistentUploadFileCommand(config);
            cmd.setVault("PersonalMedia");
            cmd.setFilePath("/home/sroy/glacier/media-1997.tar.gpg");
            cmd.execute();

            doKeyBoardInputLoop();

        } catch (Exception ex) {
            log.error("Unexpected exception", ex);
        } finally {
            HSQLDBUtil.shutdownDatabase(dataSource);
            dataSource.close();
        }
    }

    public static void doKeyBoardInputLoop() throws IOException {
        String curLine = ""; // Line read from standard in

        System.out.println("Enter a line of text (type 'quit' to exit): ");
        InputStreamReader converter = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(converter);

        while (!(curLine.equals("quit"))){
            curLine = in.readLine();

            if (!(curLine.equals("quit"))){
                System.out.println("You typed: " + curLine);
            }
        }
    }

}
