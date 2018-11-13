package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class TestProxyConfig implements ProxyConfig {

    private int frontendTcpPort = 8992, frontendTlsPort = 443;

    private boolean startMonitor = false;

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
    public boolean startMonitor() {
        return startMonitor;
    }

    @Override
    public String getServerCertChainFilePath() {
        return "/opt/cert/server.crt";
    }

    @Override
    public String getClientCertChainFilePath() {
        return "/opt/cert/client.crt";
    }

    @Override
    public String getServerKeyFilePath() {
        return "/opt/cert/pkcs8_server.key";
    }

    @Override
    public String getClientKeyFilePath() {
        return "/opt/cert/pkcs8_client.key";
    }

    @Override
    public String getRootFilePath() {
        return "/opt/cert/ca.crt";
    }

    public TestProxyConfig setFrontendTcpPort(int frontendTcpPort) {
        this.frontendTcpPort = frontendTcpPort;
        return this;
    }

    public TestProxyConfig setFrontendTlsPort(int frontendTlsPort) {
        this.frontendTlsPort = frontendTlsPort;
        return this;
    }

    public TestProxyConfig setStartMonitor(boolean startMonitor) {
        this.startMonitor = startMonitor;
        return this;
    }
}
