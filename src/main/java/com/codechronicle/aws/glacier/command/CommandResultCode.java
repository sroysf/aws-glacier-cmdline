package com.codechronicle.aws.glacier.command;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 1:46 PM
 * To change this template use File | Settings | File Templates.
 */
public enum CommandResultCode {
    SUCCESS,
    UNEXPECTED_ERROR,
    FILE_NOT_FOUND,
    FILE_UNREADABLE,
    UPLOAD_ALREADY_EXISTS
}
