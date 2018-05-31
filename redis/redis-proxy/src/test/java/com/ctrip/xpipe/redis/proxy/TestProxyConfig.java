package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class TestProxyConfig implements ProxyConfig {

    private boolean sslEnabled = false;

    @Override
    public int frontendPort() {
        return 8992;
    }

    @Override
    public long getTrafficReportIntervalMillis() {
        return 1000;
    }

    @Override
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    @Override
    public int endpointHealthCheckIntervalSec() {
        return 60;
    }

    @Override
    public String getPassword() {
        return "123456";
    }

    @Override
    public String getServerCertFilePath() {
        return "/opt/cert/xpipe-server.jks";
    }

    @Override
    public String getClientCertFilePath() {
        return "/opt/cert/xpipe-client.jks";
    }

    @Override
    public String getCertFileType() {
        return "JKS";
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }
}
