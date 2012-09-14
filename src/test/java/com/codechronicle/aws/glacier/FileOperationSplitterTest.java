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
    private File coldStartResumeTestFile;

    //TODO: Confirm proper calling of EventListeners
    //TODO: Confirm invalid parameter exceptions

    @BeforeSuite
    public void setup() {
        // Create test files
        try {
            inputFileWithRemainder = File.createTempFile("withRemainder", ".dat");
            inputFileWithPrecisePartitionSizeMultiple = File.createTempFile("precisePartitionMultiple", ".dat");
            inputSmallerThanPartSizeFile = File.createTempFile("smallerThanPartSize", ".dat");
            coldStartResumeTestFile = File.createTempFile("coldStartResumeFile",".dat");

            TestFileGenerator.writeFileOfSize(inputFileWithRemainder, (int)(PART_SIZE * 10.5));
            TestFileGenerator.writeFileOfSize(inputFileWithPrecisePartitionSizeMultiple, PART_SIZE * 10);
            TestFileGenerator.writeFileOfSize(inputSmallerThanPartSizeFile, (int)(PART_SIZE / 2));
            TestFileGenerator.writeFileOfSize(coldStartResumeTestFile, (int)(PART_SIZE * 5.5));

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
        deleteFile(coldStartResumeTestFile);
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

    @Test
    public void testColdStartResume() {
        final String jobId = "TEST_JOB_ABCD_1234";

        FileOperationSplitter fos = new FileOperationSplitter(jobId, coldStartResumeTestFile, PART_SIZE);
        fos.setFpOperator(new FilePartOperator() {

            int count = 0;

            @Override
            public void executeFilePartOperation(FilePart filePart) throws FilePartException {
                count++;
                if (count == 3) {
                    throw new FilePartException("Intentionally throwing exception on 3rd invocation", filePart);
                }
            }

            @Override
            public void executeFullFileOperation(FilePart filePart) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void fileOperationsComplete() {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void close() {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });

        try {
            fos.start();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (FilePartException e) {
            System.out.println("Caught expected exception");
        }

        String existingJobId = FileOperationSplitter.getExistingInProgressJobId(coldStartResumeTestFile);
        Assert.assertEquals(existingJobId, jobId);

        // Cleanup
        FileOperationSplitter.deleteMatchingTrackerFiles(coldStartResumeTestFile);
    }

    private void deleteFile(File file) {
        if (file != null) {
            System.out.println("Deleting file : " + file.getAbsolutePath());
            file.delete();
        }
    }
}