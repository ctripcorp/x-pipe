package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.verification.Calls;
import org.mockito.internal.verification.Times;
import org.mockito.verification.VerificationMode;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 11, 2018
 */
public class CachedNettyClientPoolTest extends AbstractTest {

    @Mock
    private SimpleObjectPool<NettyClient> objectPool;

    private CachedNettyClientPool cachedPool;

    @Before
    public void beforeCachedNettyClientPoolTest() throws BorrowObjectException {
        MockitoAnnotations.initMocks(this);
        when(objectPool.borrowObject()).thenReturn(new DefaultNettyClient(new EmbeddedChannel()));
        cachedPool = new CachedNettyClientPool(objectPool, 2);
    }

    @Test
    public void testBorrowObject() throws BorrowObjectException {
        cachedPool.borrowObject();
        cachedPool.borrowObject();
        verify(objectPool, new Times(1)).borrowObject();
    }

    @Test
    public void testBorrowObjectWithCreation() throws BorrowObjectException, TimeoutException {
        cachedPool = new CachedNettyClientPool(objectPool, 1);
        final NettyClient[] client = {null};
        for(int i = 0; i < 2; i++) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    client[0] = cachedPool.borrowObject();
                }
            });
        }
        waitConditionUntilTimeOut(()-> {return client[0] != null;}, 1000);
        verify(objectPool, new Times(2)).borrowObject();
    }

    @Test
    public void testReturnObject() throws BorrowObjectException, ReturnObjectException {
        NettyClient client = cachedPool.borrowObject();
        cachedPool.returnObject(client);
        verify(objectPool, never()).returnObject(any(NettyClient.class));

        NettyClient client1 = cachedPool.borrowObject();
        NettyClient client2 = cachedPool.borrowObject();
        Assert.assertEquals(client1, client2);
        cachedPool.returnObject(client1);
        cachedPool.returnObject(client2);
        verify(objectPool, never()).returnObject(any(NettyClient.class));
    }

    @Test
    public void testReturnMoreObjects() throws Exception {
        NettyClient client1 = cachedPool.borrowObject();
        NettyClient client2 = cachedPool.borrowObject();
        NettyClient client3 = cachedPool.borrowObject();
        Assert.assertEquals(client1, client2);
        cachedPool.returnObject(client1);
        cachedPool.returnObject(client2);
        cachedPool.returnObject(client3);
        verify(objectPool, times(1)).returnObject(any(NettyClient.class));
        verify(objectPool, times(2)).borrowObject();
    }
}