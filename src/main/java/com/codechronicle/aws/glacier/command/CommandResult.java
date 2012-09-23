package com.codechronicle.aws.glacier.command;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 1:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class CommandResult {
    private CommandResultCode resultCode = CommandResultCode.SUCCESS;
    private String message = "Success";

    public CommandResultCode getResultCode() {
        return resultCode;
    }

    public void setResultCode(CommandResultCode resultCode) {
        this.resultCode = resultCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
