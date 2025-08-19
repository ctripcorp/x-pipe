package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.client.redis.DoNothingRedisClient;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.pool.XpipeNettyClientPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSync;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParser;
import com.ctrip.xpipe.redis.core.server.*;
import com.ctrip.xpipe.redis.keeper.applier.DefaultApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author hailu
 * @date 2024/5/14 15:20
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultGapAllowReplicationTest extends AbstractRedisTest {

    @Mock
    private DefaultApplierServer applierServer;
    FakePsyncServer server;

    private NettyClient getNettyClient(DefaultGapAllowReplication replication, Endpoint endpoint) throws Exception {
        GenericObjectPool<NettyClient> pool = null;
        if(endpoint == null) {
            ConcurrentMap<Endpoint, XpipeNettyClientPool> objectPools = getFieldFrom(replication.pool, "objectPools");
            XpipeNettyClientPool xpipeNettyClientPool = objectPools.values().iterator().next();
            pool = getFieldFrom(xpipeNettyClientPool, "objectPool");
        } else {
            pool = (GenericObjectPool<NettyClient>) replication.pool.getObjectPool(endpoint);
        }
        Map<NettyClient, PooledObject<NettyClient>> allObjects = getFieldFrom(pool, "allObjects");
        return allObjects.keySet().iterator().next();
    }

    private RedisOpParser createRedisOpParse() {
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser parser = new GeneralRedisOpParser(redisOpParserManager);
        return parser;
    }

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

    private NettyClient waitPsyncNettyClientConnected(NettyClient nettyClient) throws Exception {
        waitConditionUntilTimeOut(() -> nettyClient.channel().isActive());
        return nettyClient;
    }

    private DefaultGapAllowReplication mockGapAllowReplication() throws Exception {
        DefaultGapAllowReplication psyncReplication = new DefaultGapAllowReplication(applierServer);
        psyncReplication.scheduled = scheduled;
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setJmxEnabled(false);
        config.setMaxTotal(1);
        config.setBlockWhenExhausted(true);
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(config);
        keyedObjectPool.initialize();
        psyncReplication.replId = new AtomicReference<>("?");
        psyncReplication.pool = keyedObjectPool;
        psyncReplication.offsetRecorder = new AtomicLong(0);
        psyncReplication.rdbParser = new DefaultRdbParser();
        psyncReplication.startGtidSet = new AtomicReference<>(new GtidSet(GtidSet.EMPTY_GTIDSET));
        psyncReplication.lostGtidSet = new AtomicReference<>(new GtidSet(GtidSet.EMPTY_GTIDSET));
        psyncReplication.execGtidSet = new AtomicReference<>(new GtidSet(GtidSet.EMPTY_GTIDSET));
        psyncReplication.replProto = new AtomicReference<>();
        psyncReplication.dispatcher = mockApplierCommandDispatcher(psyncReplication);
        psyncReplication.initialize();
        psyncReplication.start();
        return psyncReplication;
    }

    private ApplierCommandDispatcher mockApplierCommandDispatcher(DefaultGapAllowReplication replication) {
        DefaultCommandDispatcher dispatcher = new DefaultCommandDispatcher();
        dispatcher.sequenceController = Mockito.mock(ApplierSequenceController.class);
        dispatcher.client = new DoNothingRedisClient();
        dispatcher.parser = createRedisOpParse();
        dispatcher.stateThread =  Executors.newFixedThreadPool(1);
        dispatcher.workerThreads = Executors.newScheduledThreadPool(1);

        // 3. 注入共享的状态对象，确保 dispatcher 和 replication 操作的是同一个实例
        dispatcher.execGtidSet = replication.execGtidSet;
        dispatcher.startGtidSet = replication.startGtidSet;
        dispatcher.lostGtidSet = replication.lostGtidSet;
        dispatcher.gtidDistanceThreshold = new AtomicReference<>();
        dispatcher.offsetRecorder = replication.offsetRecorder;
        dispatcher.replId = replication.replId;

        // 4. 返回配置好的实例
        return dispatcher;
    }

    @Test
    public void doDisconnect() throws Exception {

        DefaultGapAllowReplication replication = mockGapAllowReplication();

        server = startFakePsyncServer(randomPort(), new FakePsyncHandler() {
            @Override
            public Long handlePsync(String replId, long offset) {
                return 1l;
            }

            @Override
            public byte[] genRdbData() {
                return new byte[0];
            }
        });
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());
        replication.connect(endpoint, new GtidSet(GtidSet.EMPTY_GTIDSET));

        replication.currentSync.close();

        NettyClient nettyClient = getNettyClient(replication, endpoint);

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

        DefaultGapAllowReplication replication = mockGapAllowReplication();

        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());

        replication.connect(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("mockRunId:0"));

        GapAllowedSync sync = getSuperFieldFrom(replication, "currentSync");
        waitConditionUntilTimeOut(() -> {
            try {
                AtomicLong offsetRecorder = getSuperFieldFrom(replication, "offsetRecorder");
                return offsetRecorder != null;
            } catch (Exception ignore) {}
            return false;
        });

        NettyClient nettyClient =  getNettyClient(replication, endpoint);

        nettyClient.channel().close().sync();

        Assert.assertFalse(nettyClient.channel().isActive());

        // wait reconnect
        waitConditionUntilTimeOut(() -> {
            try {
                NettyClient nettyClient1 = getNettyClient(replication, endpoint);
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

        DefaultGapAllowReplication psyncReplication = mockGapAllowReplication();
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());
        psyncReplication.connect(endpoint, new GtidSet("mockRunId:0"));

        psyncReplication.connect(null);

        waitConditionUntilTimeOut(()-> {
            try {
                return !getNettyClient(psyncReplication, endpoint).channel().isActive();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            // will not reconnect when disconnect
            waitConditionUntilTimeOut(() -> {
                try {
                    NettyClient nettyClient1 = getNettyClient(psyncReplication, endpoint);
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

        DefaultGapAllowReplication psyncReplication = mockGapAllowReplication();

        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());

        psyncReplication.connect(endpoint, new GtidSet("mockRunId:0"));

        NettyClient nettyClient = getNettyClient(psyncReplication, endpoint);

        waitPsyncNettyClientConnected(nettyClient);

        FakePsyncServer server1 = startFakePsyncServer(randomPort(), null);

        Endpoint newEndpoint = new DefaultEndPoint("127.0.0.1", server1.getPort());
        psyncReplication.connect(newEndpoint, new GtidSet("mockRunId1:0"));

        // old connection will disconnect
        waitConditionUntilTimeOut(()-> {
            try {
                return !nettyClient.channel().isActive();
            } catch (Exception e) {
                return false;
            }
        });

        // wait reconnect with new endpoint
        waitConditionUntilTimeOut(() -> {
            try {
                NettyClient nettyClient1 = getNettyClient(psyncReplication, newEndpoint);
                return nettyClient1.channel().isActive() && ((InetSocketAddress)nettyClient1.channel().remoteAddress()).getPort() == server1.getPort();
            } catch (Exception e) {
                return false;
            }
        }, 3000);

    }

    @Test
    public void reconnectAfterDisconnect() throws Exception {
        server = startFakePsyncServer(randomPort(), null);

        DefaultGapAllowReplication psyncReplication = mockGapAllowReplication();

        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());

        psyncReplication.connect(endpoint, new GtidSet("mockRunId:0"));

        NettyClient nettyClient = getNettyClient(psyncReplication, endpoint);
        waitPsyncNettyClientConnected(nettyClient);

        psyncReplication.connect(null);

        // old connection will disconnect
        waitConditionUntilTimeOut(()-> !nettyClient.channel().isActive(), 50000);

        try {
            // will not reconnect
            waitConditionUntilTimeOut(() -> {
                try {
                    NettyClient nettyClient1 = getNettyClient(psyncReplication, null);
                    return nettyClient1.channel().isActive();
                } catch (Exception e) {
                    return false;
                }
            }, 3000);
            Assert.fail();
        } catch (TimeoutException t){
            // expected
        }

        Endpoint endpoint1 = new DefaultEndPoint("127.0.0.1", server.getPort());
        psyncReplication.connect(endpoint1, new GtidSet("mockRunId:0"));

        // wait reconnect with new endpoint
        waitConditionUntilTimeOut(() -> {
            try {
                NettyClient nettyClient1 = getNettyClient(psyncReplication, endpoint1);
                return nettyClient1.channel().isActive();
            } catch (Exception e) {
                return false;
            }
        }, 3000);
    }


    @Test
    public void connectOneTimeWhenFirstConnect() throws Exception {
        server = startFakePsyncServer(randomPort(), null);

        DefaultGapAllowReplication psyncReplication = mockGapAllowReplication();

        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());

        psyncReplication.connect(endpoint, new GtidSet("mockRunId:0"));

        waitPsyncNettyClientConnected(getNettyClient(psyncReplication, endpoint));

        try {
            waitConditionUntilTimeOut(()-> server.slaveCount() > 1, 3000);
            Assert.fail();
        } catch (TimeoutException e){
            // expected
        }

    }

    @Test
    public void protoChange() throws Exception {
        int randomPort = randomPort();
        FakeXsyncServer xsyncServer = startFakeXsyncServer(randomPort, null);
        DefaultGapAllowReplication psyncReplication = mockGapAllowReplication();
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", randomPort);
        psyncReplication.connect(endpoint, new GtidSet("mockRunId:0"));
        Thread.sleep(1000);
        xsyncServer.stop();
        server = startFakePsyncServer(randomPort, null);
        Thread.sleep(2000);
        applierServer.protoChange();
        verify(applierServer, Mockito.times(1)).protoChange();
    }
}
