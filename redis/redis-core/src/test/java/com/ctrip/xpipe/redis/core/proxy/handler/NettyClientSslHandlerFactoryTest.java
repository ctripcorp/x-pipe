package com.ctrip.xpipe.redis.core.proxy.handler;

import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.context.embedded.Ssl;

import java.lang.reflect.Field;
import java.security.KeyStore;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 25, 2018
 */
public class NettyClientSslHandlerFactoryTest {

    private NettyClientSslHandlerFactory factory = new NettyClientSslHandlerFactory(new FakeTLSConfig());

    @Test
    public void testGetFilePath() {
        Assert.assertEquals("/opt/cert/xpipe-client.jks", factory.getFilePath());
    }

    @Test
    public void testCreateSslHandler() {
        ChannelHandler handler = factory.createSslHandler();
        Assert.assertTrue(handler instanceof SslHandler);
    }

    @Test
    public void testLoad() {
        KeyStore keyStore = factory.loadKeyStore();
        Assert.assertNotNull(keyStore);
    }

    @Test
    public void testReflection() {
        SslHandler sslHandler = (SslHandler) factory.createSslHandler();
        try {
            Field field = SslHandler.class.getDeclaredField("maxPacketBufferSize");
            field.setAccessible(true);
            field.set(sslHandler, 8192);
            Assert.assertEquals(field.get(sslHandler), 8192);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}