package com.ctrip.xpipe.zk.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Jul 08, 2020
 */
public class DefaultZkConfigTest {

    private DefaultZkConfig zkConfig;

    @After
    public void afterDefaultZkConfigTest() {
        // Clean up system properties to avoid test interference
        System.clearProperty("ZK.CONN.TIMEOUT");
        System.clearProperty("ZK.SESSION.TIMEOUT");
    }

    @Test
    public void testGetZkConnectionTimeoutMillisDefaultValue() {
        // Ensure property is cleared before test
        System.clearProperty("ZK.CONN.TIMEOUT");
        zkConfig = new DefaultZkConfig("localhost:2181");
        Assert.assertEquals(3000, zkConfig.getZkConnectionTimeoutMillis());
    }

    @Test
    public void testGetZkConnectionTimeoutMillis() {
        System.setProperty("ZK.CONN.TIMEOUT", "1000");
        zkConfig = new DefaultZkConfig("localhost:2181");
        Assert.assertEquals(1000, zkConfig.getZkConnectionTimeoutMillis());
    }

    @Test
    public void testGetZkSessionTimeoutMillisDefaultValue() {
        // Ensure property is cleared before test
        System.clearProperty("ZK.SESSION.TIMEOUT");
        zkConfig = new DefaultZkConfig("localhost:2181");
        Assert.assertEquals(5000, zkConfig.getZkSessionTimeoutMillis());
    }

    @Test
    public void testGetZkSessionTimeoutMillis() {
        System.setProperty("ZK.SESSION.TIMEOUT", "1000");
        zkConfig = new DefaultZkConfig("localhost:2181");
        Assert.assertEquals(1000, zkConfig.getZkSessionTimeoutMillis());
    }
}