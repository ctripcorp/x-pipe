package com.ctrip.xpipe.command;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeException;

public class CausalException extends XpipeException {

    public CausalException(String message) {
        super(message);
    }

    public CausalException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> CausalException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
