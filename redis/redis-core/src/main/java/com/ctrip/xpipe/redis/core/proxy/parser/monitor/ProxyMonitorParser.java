package com.ctrip.xpipe.redis.core.proxy.parser.monitor;

public interface ProxyMonitorParser {

    public enum Type {
        TunnelStats, SocketStats, PingStats, TrafficStats;

        public static Type parse(String typeStr) {
            Type[] types = Type.values();
            for(Type type : types) {
                if (typeStr.equalsIgnoreCase(type.name())) {
                    return type;
                }
            }
            return null;
        }
    }
}
