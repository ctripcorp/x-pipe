package com.ctrip.xpipe.redis.proxy.config;


import com.ctrip.xpipe.redis.core.config.TLSConfig;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyConfig extends TLSConfig {

    public static final int MEGA_BYTE = 1024 * 1024;

    int frontendTcpPort();

    int frontendTlsPort();

    long getTrafficReportIntervalMillis();

    int endpointHealthCheckIntervalSec();

    boolean debugTunnel();

    boolean notInterest(InetSocketAddress address);

    boolean noTlsNettyHandler();

    int getNettyWriteHighWaterMark();

    int getNettyWriteLowWaterMark();
}
