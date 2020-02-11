package com.ctrip.xpipe.exception;

/**
 * @author chen.zhu
 * <p>
 * Jan 22, 2020
 */
public class CommandNotExecuteException extends XpipeRuntimeException {

    public CommandNotExecuteException(String message) {
        super(message);
    }

    public CommandNotExecuteException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> CommandNotExecuteException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
