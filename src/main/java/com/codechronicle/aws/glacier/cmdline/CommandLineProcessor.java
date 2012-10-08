package com.codechronicle.aws.glacier.cmdline;

import com.codechronicle.aws.glacier.EnvironmentConfiguration;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

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
                File uploadFile = new File(fileCompleter.getCurrentDirectory(), tokens[1]);
                executeUploadFileCommand(uploadFile.getAbsolutePath());
            } else if (line.startsWith("quit")) {
                break;
            } else if (line.startsWith("list")) {
                executeListUploadsCommand();
            }
        }
    }

    private void executeListUploadsCommand() {
        System.out.println("Listing uploaded files");
    }

    private void executeUploadFileCommand(String filePath) {
        System.out.println("Uploading file : " + filePath);
    }

    private Collection<String> generateSupportedCommandsList() {

        ArrayList<String> commands = new ArrayList<String>();
        commands.add("upload");
        commands.add("list");
        commands.add("cd");
        commands.add("quit");
        commands.add("help");

        return commands;
    }

    private String changeDirectory(File workingDir, String line) {
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

}
