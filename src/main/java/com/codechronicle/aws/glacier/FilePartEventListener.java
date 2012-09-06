package com.codechronicle.aws.glacier;

/**
 * Created with IntelliJ IDEA.
 * User: saptarshi.roy
 * Date: 9/5/12
 * Time: 1:55 PM
 * To change this template use File | Settings | File Templates.
 */
public interface FilePartEventListener {

    void onSuccess(FilePart filePart);

    void onFailure(FilePart filePart);
}
