package com.codechronicle.aws.glacier;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/5/12
 * Time: 1:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class FilePart {

    private File file;
    private byte[] buffer;
    private int partNum;
    private int numBytes;

    public FilePart(File file, byte[] buffer, int partNum, int numBytes) {
        this.file = file;
        this.buffer = buffer;
        this.partNum = partNum;
        this.numBytes = numBytes;
    }

    public File getFile() {
        return file;
    }

    public int getPartNum() {
        return partNum;
    }

    public int getNumBytes() {
        return numBytes;
    }

    public byte[] getBuffer() {
        return buffer;
    }
}
