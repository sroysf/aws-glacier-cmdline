package com.codechronicle.aws.glacier.cmdline;

import com.codechronicle.aws.glacier.AppConstants;
import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import com.codechronicle.aws.glacier.command.*;
import com.codechronicle.aws.glacier.model.FileUploadPart;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 10/8/12
 * Time: 1:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class CommandLineProcessor {
    private EnvironmentConfiguration config;

    public CommandLineProcessor(EnvironmentConfiguration config) {
        this.config = config;
    }

    public void startProcessingUserInput() throws IOException {

        ConsoleReader consoleReader = new ConsoleReader(System.in, System.out);

        CurrentDirAwareFileNameCompleter fileCompleter = new CurrentDirAwareFileNameCompleter();
        consoleReader.addCompleter(new ArgumentCompleter(new StringsCompleter(generateSupportedCommandsList()), fileCompleter));

        while (true) {

            String line = consoleReader.readLine(fileCompleter.getCurrentDirectory() + " > ");
            String[] tokens = line.split(" ");

            if (line.startsWith("cd ")) {
                String newCurrentDir = changeDirectory(new File(fileCompleter.getCurrentDirectory()), line);
                if (newCurrentDir != null) {
                    fileCompleter.setCurrentDirectory(newCurrentDir);
                }
            } else if (line.startsWith("upload ")) {
                executeUploadFileCommand(fileCompleter.getCurrentDirectory(), tokens);
            } else if (line.startsWith("quit")) {
                cleanup();
                break;
            } else if (line.startsWith("list")) {
                executeListUploadsCommand();
            } else if (line.startsWith("details")) {
                executeGetUploadDetailsCommand(tokens);
            } else if (line.startsWith("start-uploads")) {
                startUploads();
            } else if (line.startsWith("stop-uploads")) {
                stopUploads();
            }
        }
    }

    private void cleanup() {
        stopUploads();
        UploadFileWorker.waitForWorkerThread();
    }

    private void stopUploads() {
        UploadFileWorker.stopServicingUploadQueue();
    }

    private void startUploads() {
        try {
            UploadFileWorker.startServicingUploadQueue(config);
        } catch (InterruptedException e) {
            System.out.println("Upload processing unexpectedly terminated");
        }
    }

    private void executeGetUploadDetailsCommand(String[] tokens) {

        String usage = "Usage : details <fileUploadId>";
        if (tokens.length < 2) {
            System.out.println(usage);
            return;
        }

        try {
            int id = Integer.parseInt(tokens[1]);
            GetUploadDetailsCommand cmd = new GetUploadDetailsCommand(config, id);
            cmd.execute();

            if (cmd.getResult().getResultCode() == CommandResultCode.SUCCESS) {
                List<FileUploadPart> parts = cmd.getCompletedParts();
                SimpleDateFormat sdf = new SimpleDateFormat(AppConstants.DATE_FORMAT);
                for (FileUploadPart part : parts) {
                    System.out.println(part.getPartNum() + "\t\t" + part.getStartByte() + "-" + part.getEndByte() + "\t\t" + sdf.format(part.getCompletionDate()));
                }

                System.out.println("\nPercent complete = " + cmd.getPercentComplete() + "% [" + cmd.getLastByteUploaded() + " / " + cmd.getFileUploadRecord().getLength() + "]");

            } else {
                printCommandError(cmd);
            }
        } catch (NumberFormatException nfe) {
            System.out.println(usage);
            return;
        }
    }

    private void printCommandError(GlacierCommand cmd) {
        System.out.println("Error : " + cmd.getResult().getResultCode() + " -> " + cmd.getResult().getMessage());
    }

    private void executeListUploadsCommand() {
        ListUploadsCommand cmd = new ListUploadsCommand(config);
        cmd.execute();

        if (cmd.getResult().getResultCode() == CommandResultCode.SUCCESS) {
            for (FileUploadRecord record : cmd.getRecords()) {
                System.out.println(record.getId() + "\t" + record.getVault() + "\t" + record.getStatus() + "\t" + record.getAwsArchiveId() + "\t" + record.getFilePath() );
            }
        } else {
            printCommandError(cmd);
        }
    }

    private void executeUploadFileCommand(String currentDirectory, String[] tokens) {

        if (tokens.length < 3) {
            System.out.println("Usage : upload <file-path> <vault-name>");
            return;
        }

        String filePath = tokens[1];
        String vault = tokens[2];

        File uploadFile = null;

        if (filePath.startsWith(File.separator)) {
            uploadFile = new File(filePath);
        } else if (filePath.startsWith("~")) {
            filePath = filePath.replaceAll("~", FileUtils.getUserDirectoryPath());
            uploadFile = new File(filePath);
        } else {
            uploadFile = new File(new File(currentDirectory), filePath);
        }

        System.out.println("Uploading file : " + uploadFile.getAbsolutePath() + " to vault = " + vault);

        // Everything is valid at this point, go ahead and call the command
        UploadFileCommand cmd = new UploadFileCommand(config);
        cmd.setFilePath(uploadFile.getAbsolutePath());
        cmd.setVault(vault);

        cmd.execute();

        String message = cmd.getResult().getMessage();
        switch (cmd.getResult().getResultCode()) {
            case FILE_NOT_FOUND :
                System.out.println("File not found : " + message);
                break;
            case FILE_UNREADABLE:
                System.out.println("Cannot read file : " + message);
                break;
            case UPLOAD_ALREADY_EXISTS:
                System.out.println(message);
                break;
            case SUCCESS:
                System.out.println("File queued for upload : " + uploadFile.getAbsolutePath());
                break;
            default:
                printCommandError(cmd);
        }
    }

    private Collection<String> generateSupportedCommandsList() {

        ArrayList<String> commands = new ArrayList<String>();
        commands.add("upload");
        commands.add("list");
        commands.add("cd");
        commands.add("quit");
        commands.add("help");
        commands.add("details");
        commands.add("start-uploads");
        commands.add("stop-uploads");

        return commands;
    }

    private String changeDirectory(File workingDir, String line) {
        String[] args = line.split(" ");
        String requestedPath = args[1];

        File f = null;
        if (requestedPath.startsWith("/")) {
            f = new File(requestedPath);
        } else if (requestedPath.startsWith("~")) {
            requestedPath = requestedPath.replaceAll("~", FileUtils.getUserDirectoryPath());
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

}
