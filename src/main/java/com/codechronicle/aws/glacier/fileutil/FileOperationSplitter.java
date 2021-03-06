package com.codechronicle.aws.glacier.fileutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * This class provides the functionality to split a large srcFile into component parts, delegate an operation
 * to a FilePartOperator implementation, and keep track of last successful operation. This allows callers
 * to easily pick up where they left off in the event of an error.
 *
 */
public class FileOperationSplitter {

    private File srcFile;
    private long srcFileLength;
    private RandomAccessFile raf;
    private int partitionSize;
    private FilePartOperator fpOperator;
    private int startPartNum = 1;

    private static Logger log = LoggerFactory.getLogger(FileOperationSplitter.class);

    /**
     * A unique identifier for the overall job. The status of suboperations delegated to FilePartOperator
     * implementations will be tracked using this jobId.
     */
    private String jobId;

    public FileOperationSplitter(String jobId, File srcFile, int partitionSize) {
        this.jobId = jobId;
        this.srcFile = srcFile;
        this.partitionSize = partitionSize;
    }

    public void setFpOperator(FilePartOperator fpOperator) {
        this.fpOperator = fpOperator;
    }

    public String getJobId() {
        return jobId;
    }

    public void setStartPartNum(int startPartNum) {
        this.startPartNum = startPartNum;
    }

    /**
     * Start splitting the srcFile into chunks based on partitionSize and delegate to implementations of FilePartOperator.
     *
     * If the operation was previously aborted for some reason, it will pick up where it left off.
     *
     */
    public void start() throws IOException, FilePartException {

        validateInputs();

        initFiles();

        byte[] buffer = new byte[partitionSize];

        try {
            if ((srcFileLength <= partitionSize) && (fpOperator != null)) {

                loadByteRange(buffer, 0, (int)srcFileLength);
                FilePart fullFileFP = new FilePart(srcFile, buffer, 1, (int)srcFileLength);
                fpOperator.executeFullFileOperation(fullFileFP);

            } else {

                int numParts = (int)(srcFileLength / partitionSize);

                for (int i=(startPartNum-1); i < numParts; i++) {
                    loadByteRange(buffer, (i * partitionSize), partitionSize);

                    FilePart filePart = new FilePart(srcFile, buffer, i, partitionSize);
                    delegateFilePartOperation(i, filePart);
                }

                // Now do the final part.
                int finalPartStart = (numParts * partitionSize);
                int finalPartSize = (int)(srcFileLength - finalPartStart);

                // No point in calling the delegate if it is a perfectly even partition size boundary
                if (finalPartSize > 0) {
                    loadByteRange(buffer, finalPartStart, finalPartSize);
                    FilePart lastPart = new FilePart(srcFile, buffer, numParts, finalPartSize);
                    delegateFilePartOperation(numParts, lastPart);
                }

                // Finally, report completion of file processing
                fpOperator.fileOperationsComplete();
            }
        } catch (FilePartException fpe) {
            throw fpe;
        } finally {
            if (fpOperator != null) {
                fpOperator.close();
            }
        }
    }

    private void delegateFilePartOperation(int i, FilePart filePart) throws FilePartException, IOException {
        if (fpOperator == null) {
            return;
        }

        fpOperator.executeFilePartOperation(filePart);
    }

    private byte[] loadByteRange(byte[] buffer, long offset, int numBytes) throws IOException {
        raf.seek(offset);
        raf.read(buffer, 0, numBytes);
        return buffer;
    }

    private void initFiles() throws IOException {
        raf = new RandomAccessFile(srcFile, "r");
        this.srcFileLength = srcFile.length();
    }

    private void validateInputs() throws IOException {
        if (!this.srcFile.exists()) {
            throw new IOException(srcFile.getAbsolutePath() + " does not exist");
        }

        if (!this.srcFile.canRead()) {
            throw new IOException(srcFile.getAbsolutePath() + ": unable to read");
        }

        if (this.srcFile.length() == 0) {
            throw new IOException(srcFile.getAbsolutePath() + ": empty srcFile");
        }

        if (partitionSize <= 0) {
            throw new IllegalArgumentException("Partition size must be a positive number");
        }
    }
}
