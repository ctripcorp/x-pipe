package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;

public interface SocketStatsManager {

    SocketStatsResult getSocketStatsResult(int localPort, int remotePort);

}
