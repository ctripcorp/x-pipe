package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.AbstractNettyRequestResponseCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import com.ctrip.xpipe.simpleserver.Server;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

@RunWith(MockitoJUnitRunner.class)
public class NettyTimeoutTtlListenerTest extends AbstractTest {

    @Mock
    private SimpleObjectPool<NettyClient> clientPool;

    private Server server;

    private NettyClient nettyClient;

    protected NettyKeyedPoolClientFactory factory = new NettyKeyedPoolClientFactory();

    private boolean pending = false;

    private int timeoutMill = 500;

    @Before
    public void beforeNettyTimeoutTtlListenerTest() throws Exception {
        server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() throws Exception {
                if (pending) return null;
                return "success\r\n";
            }
        });

        factory.start();

        nettyClient = factory.makeObject(new DefaultEndPoint("127.0.0.1", server.getPort())).getObject();
        waitConditionUntilTimeOut(() -> nettyClient.channel().isActive(), 1000);
        Mockito.when(clientPool.borrowObject()).thenReturn(nettyClient);
    }

    @After
    public void afterNettyTimeoutTtlListenerTest() throws Exception {
        factory.stop();
        server.stop();
    }

    @Test
    public void testNeverTimeout() throws Exception {
        sendNTime(false, 5);
        Assert.assertTrue(nettyClient.channel().isOpen());
    }

    @Test
    public void testAlwaysTimeout() throws Exception {
        timeoutMill = 1;
        sendNTime(true, 2);
        Assert.assertTrue(nettyClient.channel().isOpen());
        timeoutSend();
        waitConditionUntilTimeOut(() -> !nettyClient.channel().isOpen(), timeoutMill * 2);
    }

    private void sendNTime(boolean timeout, int n) {
        IntStream.range(0, n).forEach(i -> {
            if (timeout) timeoutSend();
            else normalSend();
        });
    }

    private void timeoutSend() {
        pending = true;
        try {
            (new TestCommand(clientPool, scheduled)).execute().sync();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof CommandTimeoutException);
        }
    }

    private void normalSend() {
        pending = false;
        try {
            (new TestCommand(clientPool, scheduled)).execute().sync();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private class TestCommand extends AbstractNettyRequestResponseCommand<String> {

        public TestCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
        }

        @Override
        protected String doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
            byteBuf.clear();
            return "success";
        }

        @Override
        public int getCommandTimeoutMilli() {
            return timeoutMill;
        }

        @Override
        public ByteBuf getRequest() {
            return Unpooled.wrappedBuffer(new byte[] {'t', 'e', 's', 't', '\r', '\n'});
        }

        @Override
        public String getName() {
            return "test";
        }

    }

}