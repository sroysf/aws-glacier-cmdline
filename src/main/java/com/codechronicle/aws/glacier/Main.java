package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.AmazonGlacier;
import com.codechronicle.aws.glacier.cmdline.CurrentDirAwareFileNameCompleter;
import com.codechronicle.aws.glacier.command.CommandResultCode;
import com.codechronicle.aws.glacier.command.ListUploadsCommand;
import com.codechronicle.aws.glacier.command.PersistentUploadFileCommand;
import com.codechronicle.aws.glacier.dbutil.HSQLDBUtil;
import com.codechronicle.aws.glacier.event.Event;
import com.codechronicle.aws.glacier.event.EventListener;
import com.codechronicle.aws.glacier.event.EventRegistry;
import com.codechronicle.aws.glacier.event.EventType;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Properties;


public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws IOException {
        ConsoleReader consoleReader = new ConsoleReader(System.in, System.out);

        CurrentDirAwareFileNameCompleter fileCompleter = new CurrentDirAwareFileNameCompleter();
        consoleReader.addCompleter(new ArgumentCompleter(new StringsCompleter("upload", "list", "cd", "quit"), fileCompleter));

        while (true) {
            String line = consoleReader.readLine(fileCompleter.getCurrentDirectory() + " > ");
            System.out.println("Command: " + line);

            String[] tokens = line.split(" ");

            if (line.startsWith("cd ")) {
                String newCurrentDir = changeDirectory(new File(fileCompleter.getCurrentDirectory()), line);
                if (newCurrentDir != null) {
                    fileCompleter.setCurrentDirectory(newCurrentDir);
                }
            } else if (line.startsWith("upload ")) {
                File uploadFile = new File(fileCompleter.getCurrentDirectory(), tokens[1]);
                System.out.println("Uploading : " + uploadFile.getAbsolutePath());
            } else if (line.startsWith("quit")) {
                break;
            }
        }
    }

    private static String changeDirectory(File workingDir, String line) {
        String[] args = line.split(" ");
        String requestedPath = args[1];

        File f = null;
        if (requestedPath.startsWith("/")) {
            f = new File(requestedPath);
        } else if (requestedPath.startsWith("~")) {
            requestedPath = requestedPath.replaceAll("~", FileUtils.getUserDirectory() + "/");
            f = new File(requestedPath);
        } else {
            f = new File(workingDir, requestedPath);
        }

        if (f.exists() && f.isDirectory()) {
            return f.getAbsolutePath();
        } else {
            return null;
        }
    }

    public static void listFiles(String[] args) throws IOException {
        Properties awsProps = new Properties();
        awsProps.load(FileUtils.openInputStream(new File(System.getenv("HOME") + "/.aws/aws.properties")));

        final AmazonGlacier client = AmazonGlacierClientFactory.getClient(awsProps);

        ComboPooledDataSource dataSource = HSQLDBUtil.initializeDatabase(null);

        try {

            EnvironmentConfiguration config = new EnvironmentConfiguration();
            config.setAwsProperties(awsProps);
            config.setClient(client);
            config.setDataSource(dataSource);

            ListUploadsCommand cmd = new ListUploadsCommand(config);
            cmd.execute();

            if (cmd.getResult().getResultCode() == CommandResultCode.SUCCESS) {
                SimpleDateFormat sdf = new SimpleDateFormat(AppConstants.DATE_FORMAT);

                System.out.println("ID\t\tVault\t\tCreated\t\tStatus\t\tFile Path");
                for (FileUploadRecord record : cmd.getRecords()) {
                    System.out.println(record.getId() + "\t\t" + record.getVault() + "\t\t" + sdf.format(record.getCreationDate()) + "\t\t" + record.getStatus() + "\t\t" + record.getFilePath());
                }
            } else {
                throw new RuntimeException("Error in command : " + cmd.getResult().getMessage());
            }

        } catch (Exception ex) {
            log.error("Unexpected exception", ex);
        } finally {
            HSQLDBUtil.shutdownDatabase(dataSource);
            dataSource.close();
        }
    }

    public static void uploadFile(String[] args) throws IOException {

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
