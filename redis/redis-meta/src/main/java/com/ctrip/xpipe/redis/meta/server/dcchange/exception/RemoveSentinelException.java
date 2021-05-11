package com.ctrip.xpipe.redis.meta.server.dcchange.exception;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerRuntimeException;

import java.net.InetSocketAddress;

public class RemoveSentinelException extends MetaServerRuntimeException {
    private static final long serialVersionUID = 1L;

    public RemoveSentinelException(InetSocketAddress sentinel, String sentinelMasterName, Throwable th) {
        super(String.format("remove sentinel:%s from master:%s fail", sentinel, sentinelMasterName), th);
    }
}
