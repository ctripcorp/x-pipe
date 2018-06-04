package com.ctrip.xpipe.redis.proxy.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultProxyConfigTest {

    ProxyConfig config = new DefaultProxyConfig();

    @Test
    public void getTrafficReportIntervalMillis() {
        Assert.assertEquals(30000, config.getTrafficReportIntervalMillis());
    }

    @Test
    public void endpointHealthCheckIntervalSec() {
        Assert.assertEquals(60, config.endpointHealthCheckIntervalSec());
    }

    @Test
    public void getPassword() {
        Assert.assertEquals("appid_xpipe", config.getPassword());
    }

    @Test
    public void getServerCertFilePath() {
        Assert.assertEquals("/opt/data/100013684/xpipe-server.jks", config.getServerCertFilePath());
    }

    @Test
    public void getClientCertFilePath() {
        Assert.assertEquals("/opt/data/100013684/xpipe-client.jks", config.getClientCertFilePath());
    }

    @Test
    public void getCertFileType() {
        Assert.assertEquals("JKS", config.getCertFileType());
    }
}