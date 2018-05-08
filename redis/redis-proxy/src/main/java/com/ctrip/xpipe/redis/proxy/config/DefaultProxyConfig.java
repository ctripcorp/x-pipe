package com.ctrip.xpipe.redis.proxy.config;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultProxyConfig implements ProxyConfig {

    @Override
    public int frontendPort() {
        return 0;
    }

    @Override
    public int workerEventLoops() {
        return 0;
    }

    @Override
    public long getTrafficReportIntervalMillis() {
        return 0;
    }

    @Override
    public int maxProxyProtocolLength() {
        return 0;
    }

    @Override
    public boolean isSslEnabled() {
        return false;
    }
}
