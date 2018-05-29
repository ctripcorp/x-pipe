package com.ctrip.xpipe.redis.proxy.config;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultProxyConfigTest {

    ProxyConfig config = new DefaultProxyConfig();

    @Test
    public void frontendPort() {
        Assert.assertEquals(9527, config.frontendPort());
    }

    @Test
    public void frontendWorkerEventLoopNum() {
        Assert.assertEquals(4, config.frontendWorkerEventLoopNum());
    }

    @Test
    public void getTrafficReportIntervalMillis() {
        Assert.assertEquals(30000, config.getTrafficReportIntervalMillis());
    }

    @Test
    public void isSslEnabled() {
        Assert.assertEquals(false, config.isSslEnabled());
    }

    @Test
    public void backendEventLoopNum() {
        Assert.assertEquals(4, config.backendEventLoopNum());
    }

    @Test
    public void endpointHealthCheckIntervalSec() {
        Assert.assertEquals(60, config.endpointHealthCheckIntervalSec());
    }

    @Test
    public void getPassword() {
        Assert.assertEquals("123456", config.getPassword());
    }

    @Test
    public void getServerCertFilePath() {
        Assert.assertEquals("/opt/cert/xpipe-server.jks", config.getServerCertFilePath());
    }

    @Test
    public void getClientCertFilePath() {
        Assert.assertEquals("/opt/cert/xpipe-client.jks", config.getClientCertFilePath());
    }

    @Test
    public void getCertFileType() {
        Assert.assertEquals("JKS", config.getCertFileType());
    }
}