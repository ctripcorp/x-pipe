package com.ctrip.xpipe.service.datasource;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

public class XPipeDataSourceDescNotFoundException extends XpipeRuntimeException {
    public XPipeDataSourceDescNotFoundException(String message) {
        super(message);
    }

    public XPipeDataSourceDescNotFoundException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> XPipeDataSourceDescNotFoundException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
