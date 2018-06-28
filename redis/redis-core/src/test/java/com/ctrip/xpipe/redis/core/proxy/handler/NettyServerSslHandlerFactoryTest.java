package com.ctrip.xpipe.redis.core.proxy.handler;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 25, 2018
 */
public class NettyServerSslHandlerFactoryTest {

    private NettyServerSslHandlerFactory factory = new NettyServerSslHandlerFactory(new FakeTLSConfig());

    @Test
    public void testGetFilePath() {
        Assert.assertEquals("/opt/cert/xpipe-server.jks", factory.getFilePath());
    }

    @Test
    public void testCreateSslHandler() {
        factory.createSslHandler();
    }
}