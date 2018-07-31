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
    public int getFixedRecvBufferSize() {
        return 1024;
    }

    @Override
    public String[] getInternalNetworkPrefix() {
        return null;
    }

    @Override
    public String getCertChainFilePath() {
        return "/opt/cert/xpipe.crt";
    }

    @Override
    public String getKeyFilePath() {
        return "/opt/cert/xpipe.key";
    }

    @Override
    public String getRootFilePath() {
        return "/opt/cert/ca.crt";
    }

    @Override
    public int getMaxPacketBufferSize() {
        return 2048;
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
