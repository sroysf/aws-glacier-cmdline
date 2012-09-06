package com.codechronicle.aws.glacier;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

public class FileOperationSplitterTest {

    private static int PART_SIZE = 4096;

    private File inputFileWithRemainder;
    private File inputSmallerThanPartSizeFile;
    private File inputFileWithPrecisePartitionSizeMultiple;

    //TODO: Confirm proper calling of EventListeners
    //TODO: Confirm invalid parameter exceptions
    //TODO: Insert mechanism for aborting midstream and then continuing where it left off
    //TODO: Confirm cleanup of temporary files

    @BeforeSuite
    public void setup() {
        // Create test files
        try {
            inputFileWithRemainder = File.createTempFile("withRemainder", ".dat");
            inputFileWithPrecisePartitionSizeMultiple = File.createTempFile("precisePartitionMultiple", ".dat");
            inputSmallerThanPartSizeFile = File.createTempFile("smallerThanPartSize", ".dat");

            writeFileOfSize(inputFileWithRemainder, (int)(PART_SIZE * 10.5));
            writeFileOfSize(inputFileWithPrecisePartitionSizeMultiple, PART_SIZE * 10);
            writeFileOfSize(inputSmallerThanPartSizeFile, (int)(PART_SIZE / 2));

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

    public FileCopyOperator testCopyFile(String jobId, File srcFile, FileCopyOperator copyOperator) throws IOException, FilePartException {
        FileOperationSplitter fos = new FileOperationSplitter(jobId, srcFile, PART_SIZE);

        fos.setFpOperator(copyOperator);
        fos.start();

        File outfile = copyOperator.getOutfile();
        System.out.println(srcFile.getAbsolutePath() + " = " + srcFile.length() + " bytes");
        System.out.println(outfile.getAbsolutePath() + " = " + outfile.length() + " bytes");
        compareFiles(srcFile, outfile);

        return copyOperator;
    }

    private void compareFiles(File srcFile, File outFile) throws IOException {
        // Compare the two files
        long srcHash = FileUtils.checksumCRC32(srcFile);
        long outHash = FileUtils.checksumCRC32(outFile);

        System.out.println("Source Hash = " + srcHash);
        System.out.println("Output Hash = " + outHash);

        Assert.assertEquals(srcHash, outHash);
    }

    @Test
    public void testFileWithRemainder () throws IOException, FilePartException {

        File outFile = null;
        String jobId = "testFileWithRemainder";
        File srcFile = inputFileWithRemainder;

        try {
            outFile = File.createTempFile(jobId, ".out");

            FileCopyOperator fco = new FileCopyOperator(outFile);
            testCopyFile(jobId, srcFile, fco);

            int expectedInvocations = (int)(srcFile.length() / PART_SIZE) + 1;
            Assert.assertEquals(fco.getNumInvocationsFilePartOperation(), expectedInvocations);
            Assert.assertEquals(fco.getNumInvocationsFullFileOperation(), 0);
        } finally {
            if (outFile != null) {
                outFile.delete();
            }
        }
    }

    @Test
    public void testFileWithPreciseSizeMultiple () throws IOException, FilePartException {

        File outFile = null;
        String jobId = "testFileWithPreciseSizeMultiple";
        File srcFile = inputFileWithPrecisePartitionSizeMultiple;

        try {
            outFile = File.createTempFile(jobId, ".out");

            FileCopyOperator fco = new FileCopyOperator(outFile);
            testCopyFile(jobId, srcFile, fco);

            int expectedInvocations = (int)(srcFile.length() / PART_SIZE);
            Assert.assertEquals(fco.getNumInvocationsFilePartOperation(), expectedInvocations);
            Assert.assertEquals(fco.getNumInvocationsFullFileOperation(), 0);
        } finally {
            if (outFile != null) {
                outFile.delete();
            }
        }

    }

    @Test
    public void testFileSmallerThanPartSize () throws IOException, FilePartException {

        File outFile = null;
        String jobId = "testFileSmallerThanPartSize";
        File srcFile = inputSmallerThanPartSizeFile;

        try {
            outFile = File.createTempFile(jobId, ".out");

            FileCopyOperator fco = new FileCopyOperator(outFile);
            testCopyFile(jobId, srcFile, fco);

            int expectedInvocations = (int)(srcFile.length() / PART_SIZE);
            Assert.assertEquals(fco.getNumInvocationsFilePartOperation(), 0);
            Assert.assertEquals(fco.getNumInvocationsFullFileOperation(), 1);
        } finally {
            if (outFile != null) {
                outFile.delete();
            }
        }
    }

    @AfterSuite
    public void cleanup() {
        deleteFile(inputSmallerThanPartSizeFile);
        deleteFile(inputFileWithRemainder);
        deleteFile(inputFileWithPrecisePartitionSizeMultiple);
    }

    //@Test
    public void testAbortRecovery () throws IOException, FilePartException {

        //FileCopyOperator fco = testCopyFile("testFileWithPrecisePartitionSizeMultiple", inputFileWithPrecisePartitionSizeMultiple);
        int totalInvocations = (int)(inputFileWithRemainder.length() / PART_SIZE);

        //fco.setAbortOnInvocationNumber((int)(totalInvocations / 2));
    }

    private void deleteFile(File file) {
        if (file != null) {
            System.out.println("Deleting file : " + file.getAbsolutePath());
            file.delete();
        }
    }
}