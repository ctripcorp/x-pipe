package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver;

import com.ctrip.xpipe.endpoint.HostPort;

public interface MetaServerManager {

    HostPort getLocalDcManagerMetaServer(long clusterId);

}
