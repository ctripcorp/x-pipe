package com.ctrip.xpipe.redis.proxy.config;


import com.ctrip.xpipe.redis.core.config.TLSConfig;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyConfig extends TLSConfig {

    int frontendTcpPort();

    int frontendTlsPort();

    long getTrafficReportIntervalMillis();

    int endpointHealthCheckIntervalSec();

    boolean noTlsNettyHandler();

    int getFixedRecvBufferSize();

    // to avoid any connect outside internal network
    String[] getInternalNetworkPrefix();

    boolean startMonitor();

    int getResponseTimeout();

}
