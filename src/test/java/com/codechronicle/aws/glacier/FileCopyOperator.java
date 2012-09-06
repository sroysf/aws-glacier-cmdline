package com.codechronicle.aws.glacier;

import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/5/12
 * Time: 4:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileCopyOperator implements FilePartOperator {

    private File outfile;
    OutputStream fos = null;

    public FileCopyOperator(File outfile) {
        this.outfile = outfile;
        try {
            fos = new FileOutputStream(outfile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void executeFilePartOperation(FilePart filePart) throws FilePartException {
        System.out.println("Executing file part operation, PART # : " + filePart.getPartNum() + ", bytes=" + filePart.getNumBytes());
        writeFilePart(filePart);
    }

    @Override
    public void executeFullFileOperation(FilePart filePart) {
        System.out.println("Executing full file operation, num bytes = " + filePart.getNumBytes());
        writeFilePart(filePart);
    }

    private void writeFilePart(FilePart filePart) {
        try {
            fos.write(filePart.getBuffer(), 0, filePart.getNumBytes());
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(fos);
    }
}
