package com.codechronicle.aws.glacier;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.*;

public class FileOperationSplitterTest {

    private static int ONE_MEGABYTE = 1024 * 1024;
    private static int PART_SIZE = ONE_MEGABYTE * 16;

    //TODO: Generation and cleanup of test files
    //TODO: Hash based comparison to ensure the files are exactly the same
    //TODO: Insert mechanism for aborting midstream and then continuing where it left off

    @Test
    public void testSmallFileCopy() throws IOException, FilePartException {
        File f = new File("/tmp/testShortInput.txt");
        FileOperationSplitter fos = new FileOperationSplitter("testjob", f, 100);

        final File outfile = new File("/tmp/testSmallOutput.txt");

        fos.setFpOperator(new FileCopyOperator(outfile));

        fos.start();
    }

    @Test
    public void testLargeFileCopy() throws IOException, FilePartException {
        File f = new File("/tmp/testInput.txt");
        FileOperationSplitter fos = new FileOperationSplitter("testjob", f, PART_SIZE);

        final File outfile = new File("/tmp/testLargeOutput.txt");

        fos.setFpOperator(new FileCopyOperator(outfile));

        fos.start();
    }
}