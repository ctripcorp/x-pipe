package com.ctrip.xpipe.redis.proxy.config;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyConfig {

    int frontendPort();

    int workerEventLoops();

    long getTrafficReportIntervalMillis();

    int maxProxyProtocolLength();

    boolean isSslEnabled();
}
