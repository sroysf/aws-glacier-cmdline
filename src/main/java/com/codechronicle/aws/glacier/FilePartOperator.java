package com.codechronicle.aws.glacier;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/5/12
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */
public interface FilePartOperator {

    void executeFilePartOperation(FilePart filePart) throws FilePartException;

    void executeFullFileOperation(FilePart filePart);

    void fileOperationsComplete();

    void close();
}
