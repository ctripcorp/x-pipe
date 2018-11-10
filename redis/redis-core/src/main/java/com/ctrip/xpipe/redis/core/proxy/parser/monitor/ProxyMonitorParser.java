package com.ctrip.xpipe.redis.core.proxy.parser.monitor;

public interface ProxyMonitorParser {

    enum Type {
        TunnelStats, SocketStats, PingStats
    }
}
