package com.ctrip.xpipe.redis.core.metaserver.impl;

public class FastMetaServerConsoleService extends DefaultMetaServerConsoleService {

    public FastMetaServerConsoleService(String metaServerAddress) {
        super(metaServerAddress, DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI, FAST_CONNECT_TIMEOUT, FAST_SO_TIMEOUT);
    }

}
