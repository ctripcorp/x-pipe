package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.pool2.PooledObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 14, 2020
 */
public class NettyClientFactoryTest extends AbstractTest {

    private NettyClientFactory factory;

    private Server server;

    @Before
    public void beforeNettyClientFactoryTest() throws Exception {
        if(server == null) {
            server = startServer("+PONG");
        }
        factory = new NettyClientFactory(new DefaultEndPoint(LOCAL_HOST, server.getPort()), false);
        factory.start();
    }

    @Test
    public void testMakeObject() throws Exception {

        PooledObject<NettyClient> pooledObject = factory.makeObject();
        NettyClient client = pooledObject.getObject();
        PooledByteBufAllocator allocator = (PooledByteBufAllocator)client.channel().alloc();
        AtomicReference<Object> freeSweepAllocationThreshold = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client.channel().eventLoop().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("[thread] {}", Thread.currentThread().getClass());
                    Field field = FieldUtils.getDeclaredField(allocator.getClass(), "threadCache", true);
                    Object threadCache = field.get(allocator);
                    logger.info("[class]{}", threadCache.getClass());
                    FastThreadLocal fastThreadLocal = (FastThreadLocal) threadCache;

                    Object poolThreadCache = fastThreadLocal.get();
                    logger.info("[class]{}", poolThreadCache.getClass());
                    Field param = FieldUtils.getField(poolThreadCache.getClass(), "freeSweepAllocationThreshold", true);
                    logger.info("[{}]", param.get(poolThreadCache));
                    freeSweepAllocationThreshold.set(param.get(poolThreadCache));
                } catch (IllegalAccessException e) {
                    logger.error("", e);
                }
                latch.countDown();
            }
        });
        latch.await();
        Assert.assertNotNull(freeSweepAllocationThreshold.get());
        Assert.assertEquals(0, freeSweepAllocationThreshold.get());

    }
}