package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;

public class FastMetaServerConsoleService extends DefaultMetaServerConsoleService {

    public FastMetaServerConsoleService(MetaserverAddress metaServerAddress) {
        super(metaServerAddress, DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI, FAST_CONNECT_TIMEOUT, FAST_SO_TIMEOUT);
    }

}
