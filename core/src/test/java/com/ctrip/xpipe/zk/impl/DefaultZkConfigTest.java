package com.ctrip.xpipe.zk.impl;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 08, 2020
 */
public class DefaultZkConfigTest {

    private DefaultZkConfig zkConfig;

    @Test
    public void testGetZkConnectionTimeoutMillisDefaultValue() {
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