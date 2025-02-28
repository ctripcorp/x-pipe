package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientPool;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.tuple.Pair;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2024/3/4
 */
public class RordbReplicationSupportTest extends AbstractRedisKeeperContextTest {

    private FakeRedisServer redisServer;

    @Before
    public void setupRordbReplicationSupportTest() throws Exception{
        this.redisServer = startFakeRedisServer();
    }

    @Test
    @Ignore
    public void testFakeRedisServerSupport() throws Exception {
        ConfigGetCommand<Boolean> cmd = new ConfigGetCommand.ConfigGetRordbSync(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort())),
                scheduled);
        Assert.assertFalse(cmd.execute().get());
        redisServer.setSupportRordb(true);
        cmd.reset();
        Assert.assertTrue(cmd.execute().get());
    }

    @Test
    @Ignore
    public void testPsyncRordbFromMaster() throws Exception {
        redisServer.setSupportRordb(true);
        InMemoryPsync psync = sendPsyncAndWaitRdbDone("127.0.0.1", redisServer.getPort(), true);
        Assert.assertArrayEquals(redisServer.getRdbContent(), psync.getRdb());
        Assert.assertEquals(RdbStore.Type.RORDB, checkRdbType(psync.getRdb()));
    }

    @Test
    public void testSlaveCapaRordbMasterSupport() throws Exception {
        redisServer.setSupportRordb(true);
        RedisKeeperServer redisKeeperServer = initKeeperAndConnectToMaster();
        InMemoryPsync psync = sendPsyncAndWaitRdbDone("127.0.0.1", redisKeeperServer.getListeningPort(), true);
        Assert.assertArrayEquals(redisServer.getRdbContent(), psync.getRdb());
        Assert.assertEquals(RdbStore.Type.RORDB, checkRdbType(psync.getRdb()));
    }

    @Test
    public void testSlaveCapaRordbMasterNotSupport() throws Exception {
        redisServer.setSupportRordb(false);
        RedisKeeperServer redisKeeperServer = initKeeperAndConnectToMaster();
        InMemoryPsync psync = sendPsyncAndWaitRdbDone("127.0.0.1", redisKeeperServer.getListeningPort(), true);
        Assert.assertArrayEquals(redisServer.getRdbContent(), psync.getRdb());
        Assert.assertEquals(RdbStore.Type.NORMAL, checkRdbType(psync.getRdb()));
    }

    @Test
    public void testSlaveRordbThenRdb() throws Exception {
        redisServer.setSupportRordb(true);
        RedisKeeperServer redisKeeperServer = initKeeperAndConnectToMaster();
        InMemoryPsync psync = sendPsyncAndWaitRdbDone("127.0.0.1", redisKeeperServer.getListeningPort(), true);
        Assert.assertArrayEquals(redisServer.getRdbContent(), psync.getRdb());
        Assert.assertEquals(RdbStore.Type.RORDB, checkRdbType(psync.getRdb()));
        psync = sendPsyncAndWaitRdbDone("127.0.0.1", redisKeeperServer.getListeningPort(), false);
        Assert.assertArrayEquals(redisServer.getRdbContent(), psync.getRdb());
        Assert.assertEquals(RdbStore.Type.NORMAL, checkRdbType(psync.getRdb()));
    }

    @Test
    public void testSlaveRdbThenRordb() throws Exception {
        redisServer.setSupportRordb(true);
        RedisKeeperServer redisKeeperServer = initKeeperAndConnectToMaster();
        InMemoryPsync psync = sendPsyncAndWaitRdbDone("127.0.0.1", redisKeeperServer.getListeningPort(), false);
        Assert.assertArrayEquals(redisServer.getRdbContent(), psync.getRdb());
        Assert.assertEquals(RdbStore.Type.NORMAL, checkRdbType(psync.getRdb()));
        sleep(100); // wait last dump clean
        psync = sendPsyncAndWaitRdbDone("127.0.0.1", redisKeeperServer.getListeningPort(), true);
        Assert.assertArrayEquals(redisServer.getRdbContent(), psync.getRdb());
        Assert.assertEquals(RdbStore.Type.RORDB, checkRdbType(psync.getRdb()));

        Assert.assertEquals(redisKeeperServer.getReplicationStore().getMetaStore().dupReplicationStoreMeta().getRordbFileSize(), psync.getRdb().length);
    }

    @Test
    public void testSlaveRordbOnRdbDumping() throws Exception {
        redisServer.setSupportRordb(true);
        RedisKeeperServer redisKeeperServer = initKeeperAndConnectToMaster();

        redisServer.setSleepBeforeSendRdb(3000);
        Pair<InMemoryPsync, InMemoryPsync> psyncs = sendPsyncOnDumpingAndWaitRdbDone("127.0.0.1",
                redisKeeperServer.getListeningPort(), false, true);
        Assert.assertEquals(RdbStore.Type.NORMAL, checkRdbType(psyncs.getKey().getRdb()));
        Assert.assertEquals(RdbStore.Type.NORMAL, checkRdbType(psyncs.getValue().getRdb()));
    }

    @Test
    public void testSlaveRdbOnRordbDumping() throws Exception {
        redisServer.setSupportRordb(true);
        RedisKeeperServer redisKeeperServer = initKeeperAndConnectToMaster();

        redisServer.setSleepBeforeSendRdb(3000);
        Pair<InMemoryPsync, InMemoryPsync> psyncs = sendPsyncOnDumpingAndWaitRdbDone("127.0.0.1",
                redisKeeperServer.getListeningPort(), true, false);
        Assert.assertEquals(RdbStore.Type.RORDB, checkRdbType(psyncs.getKey().getRdb()));
        Assert.assertEquals(RdbStore.Type.NORMAL, checkRdbType(psyncs.getValue().getRdb()));
    }

    private RdbStore.Type checkRdbType(byte[] rdb) {
        Map<String, String> auxMap = parseRdbAux(rdb);
        if (auxMap.containsKey(RdbConstant.REDIS_RDB_AUX_KEY_RORDB)) {
            return RdbStore.Type.RORDB;
        } else {
            return RdbStore.Type.NORMAL;
        }
    }

    private RedisKeeperServer initKeeperAndConnectToMaster() throws Exception {
        RedisKeeperServer redisKeeperServer = createRedisKeeperServer();
        redisKeeperServer.initialize();
        redisKeeperServer.start();

        redisKeeperServer.setRedisKeeperServerState(
                new RedisKeeperServerStateActive(redisKeeperServer, new DefaultEndPoint("127.0.0.1", redisServer.getPort())));
        redisKeeperServer.reconnectMaster();

        waitConditionUntilTimeOut(() -> redisKeeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED);

        return redisKeeperServer;
    }

    private Pair<InMemoryPsync, InMemoryPsync> sendPsyncOnDumpingAndWaitRdbDone(String redisIp, int redisPort, boolean tryRordb1, boolean tryRordb2) throws Exception {
        XpipeNettyClientPool masterPool1 = singleConnectPool(redisIp, redisPort);
        XpipeNettyClientPool masterPool2 = singleConnectPool(redisIp, redisPort);
        replConfCapa(masterPool1, tryRordb1);
        replConfCapa(masterPool2, tryRordb2);

        InMemoryPsync psync1 = new InMemoryPsync(masterPool1, "?", -1, scheduled);
        InMemoryPsync psync2 = new InMemoryPsync(masterPool2, "?", -1, scheduled);

        CountDownLatch latch = new CountDownLatch(2);
        psync1.addPsyncObserver(new TestPsyncObserver() {
            @Override
            public void endWriteRdb() {
                latch.countDown();
            }
        });
        psync2.addPsyncObserver(new TestPsyncObserver() {
            @Override
            public void endWriteRdb() {
                latch.countDown();
            }
        });

        psync1.execute();
        sleep(1000);
        psync2.execute();
        latch.await();

        return Pair.from(psync1, psync2);
    }

    private InMemoryPsync sendPsyncAndWaitRdbDone(String redisIp, int redisPort, boolean tryRordb) throws Exception {
        XpipeNettyClientPool masterPool = singleConnectPool(redisIp, redisPort);
        replConfCapa(masterPool, tryRordb);

        InMemoryPsync psync = new InMemoryPsync(masterPool, "?", -1, scheduled);
        CommandFuture future = new DefaultCommandFuture();
        psync.addPsyncObserver(new TestPsyncObserver() {
            @Override
            public void endWriteRdb() {
                future.setSuccess();
            }
        });
        psync.execute();
        future.get(10, TimeUnit.SECONDS);

        return psync;
    }

    private XpipeNettyClientPool singleConnectPool(String redisIp, int redisPort) throws Exception {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(1);
        config.setJmxEnabled(false);
        XpipeNettyClientPool pool = new XpipeNettyClientPool(new DefaultEndPoint(redisIp, redisPort), config);
        pool.initialize();
        return pool;
    }

    private void replConfCapa(XpipeNettyClientPool pool, boolean tryRordb) throws Exception {
        Replconf repl;
        if (tryRordb) {
            repl = new Replconf(pool, Replconf.ReplConfType.CAPA, scheduled,
                    CAPA.EOF.toString(), CAPA.PSYNC2.toString(), CAPA.RORDB.toString());
        } else {
            repl = new Replconf(pool, Replconf.ReplConfType.CAPA, scheduled,
                    CAPA.EOF.toString(), CAPA.PSYNC2.toString());
        }
        repl.execute().get();
    }

    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig config = new TestKeeperConfig();
        config.rdbDumpMinIntervalMilli = 0;
        return config;
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "keeper-test.xml";
    }

    class TestPsyncObserver implements PsyncObserver {
        @Override
        public void onFullSync(long masterRdbOffset) {

        }

        @Override
        public void reFullSync() {

        }

        @Override
        public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {

        }

        @Override
        public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {

        }

        @Override
        public void endWriteRdb() {

        }

        @Override
        public void onContinue(String requestReplId, String responseReplId) {

        }

        @Override
        public void onKeeperContinue(String replId, long beginOffset) {

        }
    }

}
