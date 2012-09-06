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

    @Test
    public void testAbortRecovery () throws IOException, FilePartException {

        File outFile = null;
        String jobId = "testAbortRecovery";
        File srcFile = inputFileWithPrecisePartitionSizeMultiple;

        try {
            outFile = File.createTempFile(jobId, ".out");
            int expectedInvocations = (int)(srcFile.length() / PART_SIZE);

            FileCopyOperator fco = new FileCopyOperator(outFile);
            fco.setAbortOnInvocationNumber((int)(expectedInvocations / 2));
            try {
                testCopyFile(jobId, srcFile, fco);
            } catch (RuntimeException rte) {
                // Make sure we actually aborted after 5 tries, with the right message
                Assert.assertEquals(fco.getNumInvocationsFilePartOperation(),(int)(expectedInvocations / 2));
                Assert.assertTrue(rte.getMessage().startsWith("Intentionally throwing exception"));

                // Now retry again
                fco = new FileCopyOperator(outFile); // Reopens the file stream.
                testCopyFile(jobId, srcFile, fco);

                // The second set of invocations should 1 more than half, since it includes the retry.
                Assert.assertEquals(fco.getNumInvocationsFilePartOperation(), ((int)(expectedInvocations / 2)+1));

                // Make sure no temporary files got left behind
                File[] fileListing = inputFileWithPrecisePartitionSizeMultiple.getParentFile().listFiles();
                for (File f : fileListing) {
                    String name = f.getName();
                    if (name.endsWith(jobId + ".ptracker")) {
                        throw new RuntimeException("Unexpected temp file found : " + f.getAbsolutePath());
                    }
                }
            }

            Assert.assertEquals(fco.getNumInvocationsFullFileOperation(), 0);
        } finally {
            if (outFile != null) {
                outFile.delete();
            }
        }
    }

    private void deleteFile(File file) {
        if (file != null) {
            System.out.println("Deleting file : " + file.getAbsolutePath());
            file.delete();
        }
    }
}