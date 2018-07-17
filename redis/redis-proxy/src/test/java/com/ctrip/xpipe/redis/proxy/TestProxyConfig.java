package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class TestProxyConfig implements ProxyConfig {

    private int frontendTcpPort = 8992, frontendTlsPort = 443;

    @Override
    public int frontendTcpPort() {
        return frontendTcpPort;
    }

    @Override
    public int frontendTlsPort() {
        return frontendTlsPort;
    }

    @Override
    public long getTrafficReportIntervalMillis() {
        return 1000;
    }

    @Override
    public int endpointHealthCheckIntervalSec() {
        return 60;
    }

    @Override
    public boolean noTlsNettyHandler() {
        return false;
    }

    @Override
    public String[] getInternalNetworkPrefix() {
        return null;
    }

    @Override
    public String getPassword() {
        return "100013684";
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

    public TestProxyConfig setFrontendTcpPort(int frontendTcpPort) {
        this.frontendTcpPort = frontendTcpPort;
        return this;
    }

    public TestProxyConfig setFrontendTlsPort(int frontendTlsPort) {
        this.frontendTlsPort = frontendTlsPort;
        return this;
    }
}
