package com.codechronicle.aws.glacier;

import com.amazonaws.services.glacier.TreeHashGenerator;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileOperationSplitterTest {

    private static int PART_SIZE = 4096;

    private File inputFileWithRemainder;
    private File inputFileWithPrecisePartitionSizeMultiple;

    //TODO: Insert mechanism for aborting midstream and then continuing where it left off

    @BeforeSuite
    public void setup() {
        // Create test files
        try {
            inputFileWithRemainder = File.createTempFile("withRemainder", ".dat");
            inputFileWithPrecisePartitionSizeMultiple = File.createTempFile("precisePartitionMultiple", ".dat");

            writeFileOfSize(inputFileWithRemainder, (int)(PART_SIZE * 10.5));
            writeFileOfSize(inputFileWithPrecisePartitionSizeMultiple, PART_SIZE * 10);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private void writeFileOfSize(File outputFile, int numBytes) throws IOException {
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

    public void testCopyFile(String jobId, File srcFile) throws IOException, FilePartException {
        FileOperationSplitter fos = new FileOperationSplitter(jobId, srcFile, PART_SIZE);

        File outFile = null;

        try {

            outFile = File.createTempFile(jobId, ".out");

            FileCopyOperator copyOperator = new FileCopyOperator(outFile);
            fos.setFpOperator(copyOperator);
            fos.start();

            System.out.println(srcFile.getAbsolutePath() + " = " + srcFile.length() + " bytes");
            System.out.println(outFile.getAbsolutePath() + " = " + outFile.length() + " bytes");

            // Compare the two files
            long srcHash = FileUtils.checksumCRC32(srcFile);
            long outHash = FileUtils.checksumCRC32(outFile);

            System.out.println("Source Hash = " + srcHash);
            System.out.println("Output Hash = " + outHash);

            Assert.assertEquals(srcHash, outHash);

        } catch (IOException io) {
            throw io;
        } finally {
            if (outFile != null) {
                outFile.delete();
            }
        }
    }

    @Test
    public void testFileWithRemainder () throws IOException, FilePartException {
        testCopyFile("testFileWithRemainder", inputFileWithRemainder);
    }

    @Test
    public void testFileWithPreciseSizeMultiple () throws IOException, FilePartException {
        testCopyFile("testFileWithPrecisePartitionSizeMultiple", inputFileWithPrecisePartitionSizeMultiple);
    }

    @AfterSuite
    public void cleanup() {
        if (inputFileWithRemainder != null) {
            System.out.println("Deleting file : " + inputFileWithRemainder.getAbsolutePath());
            inputFileWithRemainder.delete();
        }

        if (inputFileWithPrecisePartitionSizeMultiple != null) {
            System.out.println("Deleting file : " + inputFileWithPrecisePartitionSizeMultiple.getAbsolutePath());
            inputFileWithPrecisePartitionSizeMultiple.delete();
        }
    }
}