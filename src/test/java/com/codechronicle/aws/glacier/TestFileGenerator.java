package com.codechronicle.aws.glacier;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/9/12
 * Time: 4:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestFileGenerator {

    public static void writeFileOfSize(File outputFile, int numBytes) throws IOException {
        BufferedOutputStream bos = null;

        try {
            bos = new BufferedOutputStream(FileUtils.openOutputStream(outputFile));

            byte b = (byte)(255 * Math.random());
            for (int i=0; i<numBytes; i++) {
                bos.write(b);
            }

        } catch (IOException ie) {
            throw ie;
        } finally {
            if (bos != null) {
                IOUtils.closeQuietly(bos);
            }
        }
    }
}
