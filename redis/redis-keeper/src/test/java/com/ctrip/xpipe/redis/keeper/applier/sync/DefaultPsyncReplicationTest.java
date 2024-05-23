package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.protocal.cmd.ApplierPsync;
import com.ctrip.xpipe.redis.core.redis.DefaultRunIdGenerator;
import com.ctrip.xpipe.redis.core.server.FakePsyncHandler;
import com.ctrip.xpipe.redis.core.server.FakePsyncServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * @author hailu
 * @date 2024/5/14 15:20
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultPsyncReplicationTest extends AbstractRedisTest {

    FakePsyncServer server;

    private <T> T getSuperFieldFrom(Object obj, String fieldName) throws Exception {
        Field declaredField = obj.getClass().getSuperclass().getDeclaredField(fieldName);
        declaredField.setAccessible(true);
        return (T) declaredField.get(obj);
    }

    private <T> T getFieldFrom(Object obj, String fieldName) throws Exception {
        Field declaredField = obj.getClass().getDeclaredField(fieldName);
        declaredField.setAccessible(true);
        return (T) declaredField.get(obj);
    }

    private NettyClient waitPsyncNettyClientConnected(Sync sync) throws Exception {
        NettyClient nettyClient = getSuperFieldFrom(sync, "nettyClient");
        waitConditionUntilTimeOut(() -> nettyClient.channel().isActive());
        return nettyClient;
    }

    private DefaultPsyncReplication mockPsyncReplication() throws Exception {
        DefaultPsyncReplication psyncReplication = new DefaultPsyncReplication(mock(ApplierServer.class));
        psyncReplication.scheduled = scheduled;
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool();
        keyedObjectPool.initialize();
        psyncReplication.pool = keyedObjectPool;
        psyncReplication.dispatcher = new DefaultCommandDispatcher();
        psyncReplication.offsetRecorder = new AtomicLong(0);
        psyncReplication.initialize();
        psyncReplication.start();
        return psyncReplication;
    }

    @Test
    public void doDisconnect() throws Exception {
        server = startFakePsyncServer(randomPort(), null);

        Sync sync = new ApplierPsync("127.0.0.1", server.getPort(), new AtomicReference<>(DefaultRunIdGenerator.DEFAULT.generateRunid()), new AtomicLong(-1), scheduled);
        sync.execute();

        NettyClient nettyClient = waitPsyncNettyClientConnected(sync);

        sync.close();

        waitConditionUntilTimeOut(() -> !nettyClient.channel().isActive(), 3000);
    }

    @Test
    public void reconnectWhenChannelClose() throws Exception {

        AtomicBoolean serverAssertError = new AtomicBoolean(false);
        AtomicBoolean serverRecvTwice = new AtomicBoolean(false);

        server = startFakePsyncServer(randomPort(), new FakePsyncHandler() {

            public boolean firstCalled = true;

            @Override
            public Long handlePsync(String replId, long offset) {
                if (firstCalled) {
                    firstCalled = false;
                    return 666L;
                }
                serverRecvTwice.set(true);
                return 999L;
            }

            @Override
            public byte[] genRdbData() {
                return new byte[0];
            }
        });

        DefaultPsyncReplication psyncReplication = mockPsyncReplication();

        psyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Sync sync = getSuperFieldFrom(psyncReplication, "currentSync");
        waitConditionUntilTimeOut(() -> {
            try {
                AtomicLong offsetRecorder = getSuperFieldFrom(psyncReplication, "offsetRecorder");
                return offsetRecorder != null;
            } catch (Exception ignore) {}
            return false;
        });

        NettyClient nettyClient = waitPsyncNettyClientConnected(sync);

        nettyClient.channel().close().sync();

        Assert.assertFalse(nettyClient.channel().isActive());

        // wait reconnect
        waitConditionUntilTimeOut(() -> {
            try {
                final Sync currentSync = getSuperFieldFrom(psyncReplication, "currentSync");
                NettyClient nettyClient1 = getSuperFieldFrom(currentSync, "nettyClient");
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
        server = startFakePsyncServer(randomPort(), null);

        DefaultPsyncReplication psyncReplication = mockPsyncReplication();

        psyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Sync sync = getSuperFieldFrom(psyncReplication, "currentSync");

        NettyClient nettyClient = waitPsyncNettyClientConnected(sync);

        psyncReplication.connect(null);

        waitConditionUntilTimeOut(()-> !nettyClient.channel().isActive());

        try {
            // will not reconnect when disconnect
            waitConditionUntilTimeOut(() -> {
                try {
                    final Sync currentSync = getSuperFieldFrom(psyncReplication, "currentSync");
                    NettyClient nettyClient1 = getSuperFieldFrom(currentSync, "nettyClient");
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
        server = startFakePsyncServer(randomPort(), null);

        DefaultPsyncReplication psyncReplication = mockPsyncReplication();

        psyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Sync sync = getSuperFieldFrom(psyncReplication, "currentSync");

        NettyClient nettyClient = waitPsyncNettyClientConnected(sync);

        FakePsyncServer server1 = startFakePsyncServer(randomPort(), null);

        psyncReplication.connect(new DefaultEndPoint("127.0.0.1", server1.getPort()), new GtidSet("mockRunId1:0"));

        // old connection will disconnect
        waitConditionUntilTimeOut(()-> !nettyClient.channel().isActive());

        // wait reconnect with new endpoint
        waitConditionUntilTimeOut(() -> {
            try {
                final Sync currentSync = getSuperFieldFrom(psyncReplication, "currentSync");
                NettyClient nettyClient1 = getSuperFieldFrom(currentSync, "nettyClient");
                return nettyClient1.channel().isActive() && ((InetSocketAddress)nettyClient1.channel().remoteAddress()).getPort() == server1.getPort();
            } catch (Exception e) {
                return false;
            }
        }, 3000);

    }

    @Test
    public void reconnectAfterDisconnect() throws Exception {
        server = startFakePsyncServer(randomPort(), null);

        DefaultPsyncReplication psyncReplication = mockPsyncReplication();

        psyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Sync sync = getSuperFieldFrom(psyncReplication, "currentSync");

        NettyClient nettyClient = waitPsyncNettyClientConnected(sync);

        psyncReplication.connect(null);

        // old connection will disconnect
        waitConditionUntilTimeOut(()-> !nettyClient.channel().isActive());

        try {
            // will not reconnect
            waitConditionUntilTimeOut(() -> {
                try {
                    final Sync currentSync = getSuperFieldFrom(psyncReplication, "currentSync");
                    NettyClient nettyClient1 = getSuperFieldFrom(currentSync, "nettyClient");
                    return nettyClient1.channel().isActive();
                } catch (Exception e) {
                    return false;
                }
            }, 3000);
            Assert.fail();
        } catch (TimeoutException t){
            // expected
        }

        psyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        // wait reconnect with new endpoint
        waitConditionUntilTimeOut(() -> {
            try {
                final Sync currentSync = getSuperFieldFrom(psyncReplication, "currentSync");
                NettyClient nettyClient1 = getSuperFieldFrom(currentSync, "nettyClient");
                return nettyClient1.channel().isActive();
            } catch (Exception e) {
                return false;
            }
        }, 3000);
    }


    @Test
    public void connectOneTimeWhenFirstConnect() throws Exception {
        server = startFakePsyncServer(randomPort(), null);

        DefaultPsyncReplication psyncReplication = mockPsyncReplication();

        psyncReplication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        Sync sync = getSuperFieldFrom(psyncReplication, "currentSync");

        waitPsyncNettyClientConnected(sync);

        try {
            waitConditionUntilTimeOut(()-> server.slaveCount() > 1, 3000);
            Assert.fail();
        } catch (TimeoutException e){
            // expected
        }

    }
}
