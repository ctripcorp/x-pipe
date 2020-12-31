package com.ctrip.xpipe.redis.console.migration.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class MigrationUnderProcessingException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public MigrationUnderProcessingException(long eventId) {
        super(String.format("%d under processing, skip", eventId));
    }

}
