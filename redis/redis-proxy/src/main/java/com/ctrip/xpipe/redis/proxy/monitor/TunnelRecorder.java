package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.redis.proxy.Tunnel;

public interface TunnelRecorder {

    void record(Tunnel tunnel);
}
