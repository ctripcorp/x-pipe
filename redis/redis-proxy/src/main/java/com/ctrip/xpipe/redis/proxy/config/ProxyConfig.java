package com.ctrip.xpipe.redis.proxy.config;


import com.ctrip.xpipe.redis.core.config.TLSConfig;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyConfig extends TLSConfig {

    int frontendPort();

    int frontendWorkerEventLoopNum();

    long getTrafficReportIntervalMillis();

    boolean isSslEnabled();

    int backendEventLoopNum();

    int endpointHealthCheckIntervalSec();
}
