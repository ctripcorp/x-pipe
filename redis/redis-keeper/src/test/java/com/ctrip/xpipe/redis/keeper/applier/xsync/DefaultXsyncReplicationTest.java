package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.server.FakeXsyncHandler;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;

/**
 * @author: cchen6
 * 2022/10/28
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultXsyncReplicationTest extends AbstractRedisTest {

    FakeXsyncServer server;

    private <T> T getFieldFrom(Object obj, String fieldName) throws Exception {
        Field declaredField = obj.getClass().getDeclaredField(fieldName);
        declaredField.setAccessible(true);
        return (T) declaredField.get(obj);
    }

    private NettyClient waitXsyncNettyClientConnected(Xsync xsync) throws Exception {
        NettyClient nettyClient = getFieldFrom(xsync, "nettyClient");
        waitConditionUntilTimeOut(() -> nettyClient.channel().isActive());
        return nettyClient;
    }

    private DefaultXsyncReplication mockXsyncReplication() throws Exception {
        DefaultXsyncReplication xsyncReplication = new DefaultXsyncReplication(mock(ApplierServer.class));
        xsyncReplication.scheduled = scheduled;
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool();
        keyedObjectPool.initialize();
        xsyncReplication.pool = keyedObjectPool;
        xsyncReplication.dispatcher = new DefaultCommandDispatcher();
        xsyncReplication.offsetRecorder = new AtomicLong(0);
        xsyncReplication.initialize();
        xsyncReplication.start();
        return xsyncReplication;
    }

    @Test
    public void doDisconnect() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);

        Xsync xsync = new DefaultXsync("127.0.0.1", server.getPort(), new GtidSet("mockRunid:0"), null, scheduled);
        xsync.execute();

        NettyClient nettyClient = waitXsyncNettyClientConnected(xsync);

        xsync.close();

        waitConditionUntilTimeOut(() -> !nettyClient.channel().isActive(), 3000);
    }

    @Test
    public void reconnectWhenChannelClose() throws Exception {

        AtomicBoolean serverAssertError = new AtomicBoolean(false);
        AtomicBoolean serverRecvTwice = new AtomicBoolean(false);

        server = startFakeXsyncServer(randomPort(), new FakeXsyncHandler() {

            public boolean firstCalled = true;

            @Override
            public GtidSet handleXsync(List<String> interestedSidno, GtidSet excludedGtidSet, Object excludedVectorClock) {
                if (firstCalled) {
                    firstCalled = false;
                    return new GtidSet("mockRunId:0");
                }
                if (!excludedGtidSet.toString().equals("mockRunId:1-10")) {
                    serverAssertError.set(true);
                }
                serverRecvTwice.set(true);
                return new GtidSet("mockRunId:0");
            }

            @Override
            public byte[] genRdbData() {
                return new byte[0];
            }
        });

        DefaultXsyncReplication xsyncReplication = mockXsyncReplication();

        xsyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Xsync xsync = getFieldFrom(xsyncReplication, "currentXsync");

        waitConditionUntilTimeOut(()-> {
            try {
                GtidSet gtid_received = getFieldFrom(xsyncReplication.dispatcher, "gtid_received");
                return gtid_received != null;
            } catch (Exception ignore) {}
            return false;
        });

        GtidSet gtid_received = getFieldFrom(xsyncReplication.dispatcher, "gtid_received");
        gtid_received.rise("mockRunId:10");

        NettyClient nettyClient = waitXsyncNettyClientConnected(xsync);

        nettyClient.channel().close().sync();

        Assert.assertFalse(nettyClient.channel().isActive());

        // wait reconnect
        waitConditionUntilTimeOut(() -> {
            try {
                final Xsync currentXsync = getFieldFrom(xsyncReplication, "currentXsync");
                NettyClient nettyClient1 = getFieldFrom(currentXsync, "nettyClient");
                return nettyClient1.channel().isActive();
            } catch (Exception e) {
                return false;
            }
        }, 3000);

        waitConditionUntilTimeOut(serverRecvTwice::get);

        Assert.assertFalse(serverAssertError.get());
    }

    @Test
    public void notReconnectWhenDisconnect() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);

        DefaultXsyncReplication xsyncReplication = mockXsyncReplication();

        xsyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Xsync xsync = getFieldFrom(xsyncReplication, "currentXsync");

        NettyClient nettyClient = waitXsyncNettyClientConnected(xsync);

        xsyncReplication.connect(null);

        waitConditionUntilTimeOut(()-> !nettyClient.channel().isActive());

        try {
            // will not reconnect when disconnect
            waitConditionUntilTimeOut(() -> {
                try {
                    final Xsync currentXsync = getFieldFrom(xsyncReplication, "currentXsync");
                    NettyClient nettyClient1 = getFieldFrom(currentXsync, "nettyClient");
                    return nettyClient1.channel().isActive();
                } catch (Exception e) {
                    return false;
                }
            }, 3000);
            Assert.fail();
        } catch (TimeoutException t){
            //expected
        }
    }

    @Test
    public void reconnectWhenTargetChange() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);

        DefaultXsyncReplication xsyncReplication = mockXsyncReplication();

        xsyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Xsync xsync = getFieldFrom(xsyncReplication, "currentXsync");

        NettyClient nettyClient = waitXsyncNettyClientConnected(xsync);

        FakeXsyncServer server1 = startFakeXsyncServer(randomPort(), null);

        xsyncReplication.connect(new DefaultEndPoint("127.0.0.1", server1.getPort()), new GtidSet("mockRunId1:0"));

        // old connection will disconnect
        waitConditionUntilTimeOut(()-> !nettyClient.channel().isActive());

        // wait reconnect with new endpoint
        waitConditionUntilTimeOut(() -> {
            try {
                final Xsync currentXsync = getFieldFrom(xsyncReplication, "currentXsync");
                NettyClient nettyClient1 = getFieldFrom(currentXsync, "nettyClient");
                return nettyClient1.channel().isActive() && ((InetSocketAddress)nettyClient1.channel().remoteAddress()).getPort() == server1.getPort();
            } catch (Exception e) {
                return false;
            }
        }, 3000);

    }

    @Test
    public void reconnectAfterDisconnect() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);

        DefaultXsyncReplication xsyncReplication = mockXsyncReplication();

        xsyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Xsync xsync = getFieldFrom(xsyncReplication, "currentXsync");

        NettyClient nettyClient = waitXsyncNettyClientConnected(xsync);

        xsyncReplication.connect(null);

        // old connection will disconnect
        waitConditionUntilTimeOut(()-> !nettyClient.channel().isActive());

        try {
            // will not reconnect
            waitConditionUntilTimeOut(() -> {
                try {
                    final Xsync currentXsync = getFieldFrom(xsyncReplication, "currentXsync");
                    NettyClient nettyClient1 = getFieldFrom(currentXsync, "nettyClient");
                    return nettyClient1.channel().isActive();
                } catch (Exception e) {
                    return false;
                }
            }, 3000);
            Assert.fail();
        } catch (TimeoutException t){
            // expected
        }

        xsyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        // wait reconnect with new endpoint
        waitConditionUntilTimeOut(() -> {
            try {
                final Xsync currentXsync = getFieldFrom(xsyncReplication, "currentXsync");
                NettyClient nettyClient1 = getFieldFrom(currentXsync, "nettyClient");
                return nettyClient1.channel().isActive();
            } catch (Exception e) {
                return false;
            }
        }, 3000);
    }


    @Test
    public void connectOneTimeWhenFirstConnect() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);

        DefaultXsyncReplication xsyncReplication = mockXsyncReplication();

        xsyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Xsync xsync = getFieldFrom(xsyncReplication, "currentXsync");

        waitXsyncNettyClientConnected(xsync);

        try {
            waitConditionUntilTimeOut(()-> server.slaveCount() > 1, 3000);
            Assert.fail();
        } catch (TimeoutException e){
            // expected
        }

    }
}