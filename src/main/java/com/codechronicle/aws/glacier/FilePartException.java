package com.codechronicle.aws.glacier;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/5/12
 * Time: 1:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class FilePartException extends Exception {
    private FilePart filePart;

    public FilePartException(String msg, FilePart filePart, Throwable throwable) {
        super(generateMessage(msg, filePart), throwable);
        this.filePart = filePart;
    }

    public FilePartException(String msg, FilePart filePart) {
        super(generateMessage(msg, filePart));

        this.filePart = filePart;
    }

    private static String generateMessage(String msg, FilePart filePart) {
        return filePart.getFile().getAbsolutePath()
                + "[" + filePart.getPartNum()
                + ","
                + filePart.getNumBytes()
                + "] "
                + msg;
    }
}
