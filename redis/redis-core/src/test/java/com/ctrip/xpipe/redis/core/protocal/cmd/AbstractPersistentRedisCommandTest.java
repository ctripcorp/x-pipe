package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.AbstractNettyCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Aug 11, 2018
 */
public class AbstractPersistentRedisCommandTest extends AbstractRedisTest {

    @Test
    public void testNettyClientReturnable() throws Exception {
        Server server = startEmptyServer();
        AtomicReference<NettyClient> clientReference = new AtomicReference<>();
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool().
                getKeyPool(new DefaultEndPoint("localhost", server.getPort()));
        int N = 5;
        for(int i = 0; i < N; i++) {
            AbstractNettyCommand<Void> command = new TestNettyClientReturnableCommand(clientPool, scheduled, clientReference,
                    (nettyClient, reference)->Assert.assertNotEquals(nettyClient, reference.getAndSet(nettyClient)));
            command.execute();
        }
        server.stop();
    }


    @Test
    public void testNettyClientReturnAfterDone() throws Exception {
        Server server = startEmptyServer();
        AtomicReference<NettyClient> clientReference = new AtomicReference<>();
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool().
                getKeyPool(new DefaultEndPoint("localhost", server.getPort()));
        int N = 5;
        for(int i = 0; i < N; i++) {
            AbstractNettyCommand<Void> command = new TestNettyClientReturnableCommand(clientPool, scheduled, clientReference,
                    (nettyClient, reference)->{
                        NettyClient oldOne = reference.getAndSet(nettyClient);
                        if(oldOne != null) {
                            Assert.assertEquals(nettyClient, oldOne);
                        }
                    });
            command.execute();
            command.future().setSuccess();
        }
        server.stop();
    }

    class TestNettyClientReturnableCommand extends AbstractPersistentRedisCommand<Void> {

        private AtomicReference<NettyClient> clientReference;
        private BiConsumer<NettyClient, AtomicReference<NettyClient>> consumer;


        public TestNettyClientReturnableCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                                                AtomicReference<NettyClient> clientReference,
                                                BiConsumer<NettyClient, AtomicReference<NettyClient>> consumer) {
            super(clientPool, scheduled);
            this.clientReference = clientReference;
            this.consumer = consumer;
        }


        @Override
        protected void doSendRequest(NettyClient nettyClient, ByteBuf byteBuf) {
            consumer.accept(nettyClient, clientReference);
        }


        @Override
        public ByteBuf getRequest() {
            return null;
        }


        @Override
        protected Void format(Object payload) {
            return null;
        }


        @Override
        public String getName() {
            return null;
        }

    }
}