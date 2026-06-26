package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.SetValueCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

/**
 * Integration test reproducing the cmd-file-truncation psync offset bug.
 *
 * Bug:
 *   buildIndexFromCmdFile detects an incomplete transaction and truncates the cmd file
 *   on disk, but CommandFileContext.fileLength (AtomicLong) is not updated.
 *   locateTailOfCmd() returns the stale (pre-truncation) totalLength to the upstream
 *   master as the psync offset.  The master replies +CONTINUE from that offset and
 *   begins streaming data starting inside a previous command's binary value.
 *   DefaultCommandStore logs: [onCommand][err] ... NumberFormatException: "$10"
 *   The downstream redis slave ends up with corrupted or missing data.
 *
 * Fix:
 *   syncCommandWriterOffset() calls truncateCmdFileTo() right after setLength(), so
 *   CommandWriter.totalLength() always reflects the truncated file size.
 */
public class KeeperCmdFileTruncatePsyncTest extends AbstractKeeperIntegratedSingleDc {

    private List<RedisKeeperServer> redisKeeperServers = new LinkedList<>();

    /**
     * Scenario:
     * 1.  Enable GTID on master, write 50 keys so the keeper accumulates real cmd data.
     * 2.  Start active keeper (GTID/xsync mode) and connect it to master.
     * 3.  Start a downstream redis slave pointing at the active keeper; wait for sync.
     * 4.  Stop the active keeper, append an incomplete MULTI transaction to its cmd file.
     * 5.  Restart the active keeper (same data dir).
     *     - Recovery (buildIndexFromCmdFile) must truncate the garbage and, crucially,
     *       update CommandWriter.totalLength() to match the truncated length.
     * 6.  The downstream slave reconnects via xsync (partial sync, NOT full sync).
     * 7.  Write 20 more keys.
     * 8.  Assert: slave gtid_set == master gtid_set; no extra full resync occurred.
     */
    @Test
    public void testKeeperRestartAfterCmdFileTruncationDoesNotCauseOnCommandErr() throws Exception {
        // Step 1: Enable GTID and write initial data
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());
        for (int i = 0; i < 50; i++) {
            writeKey("init_key_" + i);
        }
        logger.info("[test] initial data written");

        // Step 2: Start keepers
        initKeepers();
        logger.info("[test] keepers started and connected to master");

        // Step 3: Start a downstream redis slave pointed at active keeper
        int slavePort = randomPort();
        RedisMeta slaveMeta = new RedisMeta().setIp("127.0.0.1").setPort(slavePort);
        super.startRedis(slaveMeta);
        slaveOfKeeper("127.0.0.1", slavePort);
        waitSlaveOnline("127.0.0.1", slavePort);
        setRedisToGtidEnabled(slaveMeta.getIp(), slaveMeta.getPort());
        Thread.sleep(500);
        logger.info("[test] downstream slave online at port {}", slavePort);

        int fullSyncsBefore = getActiveKeeperFullSyncCount();

        // Step 4: Get the cmd file, stop the active keeper, corrupt the file
        RedisKeeperServer activeKeeperServer = getRedisKeeperServer(activeKeeper);
        File cmdFile = getActiveCmdFile(activeKeeperServer);
        long cleanLength = cmdFile.length();
        logger.info("[test] cmd file: {}, clean length: {}", cmdFile.getName(), cleanLength);

        activeKeeperServer.stop();
        activeKeeperServer.dispose();
        remove(activeKeeperServer);
        logger.info("[test] active keeper stopped");

        byte[] incompleteTx = buildIncompleteMultiTransaction("crash_key", "crash_value");
        try (RandomAccessFile raf = new RandomAccessFile(cmdFile, "rw")) {
            raf.seek(cmdFile.length());
            raf.write(incompleteTx);
        }
        Assert.assertTrue("incomplete tx must grow the file",
                cmdFile.length() > cleanLength);
        logger.info("[test] appended {} bytes of incomplete MULTI tx", incompleteTx.length);

        // Step 5: Restart the active keeper with the same data dir
        RedisKeeperServer restartedKeeper = startKeeper(activeKeeper);
        setKeeperState(activeKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
        waitConditionUntilTimeOut(() ->
                restartedKeeper.getRedisMaster() != null
                        && restartedKeeper.getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        logger.info("[test] keeper restarted and reconnected to master");

        // Verify the fix: cmd file truncated back; CommandWriter offset must be consistent
        AbstractCommandStore recoveredCmdStore = (AbstractCommandStore)
                ((DefaultReplicationStore) restartedKeeper.getReplicationStore()).getCommandStore();
        CommandWriter recoveredWriter = recoveredCmdStore.getCommandWriter();
        File recoveredCmdFile = recoveredWriter.getFileContext().getCommandFile().getFile();

        Assert.assertEquals(
                "cmd file must be truncated back to the last complete command boundary",
                cleanLength, recoveredCmdFile.length());

        long fileStartOffset = recoveredWriter.getFileContext().getCommandFile().getStartOffset();
        long expectedTotalLength = fileStartOffset + cleanLength;
        Assert.assertEquals(
                "CommandWriter.totalLength() must equal startOffset + truncatedLength; "
                        + "a stale value here causes master to resume data from inside a payload, "
                        + "triggering [onCommand][err] in DefaultCommandStore",
                expectedTotalLength, recoveredWriter.totalLength());

        // Step 6: Wait for slave to re-link via xsync (partial sync)
        waitSlaveOnline("127.0.0.1", slavePort);
        Thread.sleep(1000);

        // Step 7: Write more data through the restarted keeper
        for (int i = 0; i < 20; i++) {
            writeKey("post_restart_key_" + i);
        }
        Thread.sleep(2000);

        // Step 8: Verify no extra full resync and that gtid sets are in sync
        int fullSyncsAfter = getActiveKeeperFullSyncCount();
        Assert.assertEquals(
                "slave must reconnect via xsync partial sync, not full sync, after keeper restart",
                fullSyncsBefore, fullSyncsAfter);

        String masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort(), "gtid_set");
        String slaveGtid  = getGtidSet("127.0.0.1", slavePort, "gtid_set");
        Assert.assertEquals(
                "slave gtid_set must match master after incremental sync through restarted keeper",
                masterGtid, slaveGtid);
    }

    // ---- helpers ----

    private void initKeepers() throws Exception {
        redisKeeperServers.add(getRedisKeeperServer(activeKeeper));
        redisKeeperServers.add(getRedisKeeperServer(backupKeeper));
        setKeeperState(activeKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
        setKeeperState(backupKeeper, KeeperState.ACTIVE, activeKeeper.getIp(), activeKeeper.getPort());
        for (RedisKeeperServer ks : redisKeeperServers) {
            waitConditionUntilTimeOut(() ->
                    ks.getRedisMaster() != null && ks.getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }
    }

    private int getActiveKeeperFullSyncCount() {
        RedisKeeperServer ks = getRedisKeeperServer(activeKeeper);
        if (ks == null) return 0;
        return (int) ks.getKeeperMonitor().getKeeperStats().getFullSyncCount();
    }

    private File getActiveCmdFile(RedisKeeperServer keeperServer) {
        AbstractCommandStore cmdStore = (AbstractCommandStore)
                ((DefaultReplicationStore) keeperServer.getReplicationStore()).getCommandStore();
        return cmdStore.getCommandWriter().getFileContext().getCommandFile().getFile();
    }

    private void slaveOfKeeper(String ip, int port) throws Exception {
        SimpleObjectPool<NettyClient> pool =
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        new SlaveOfCommand(pool, activeKeeper.getIp(), activeKeeper.getPort(), scheduled).execute().get();
    }

    private void writeKey(String key) throws Exception {
        new SetValueCommand(redisMaster.getIp(), redisMaster.getPort(), scheduled, key, "test_value").execute().get();
    }

    /** Builds a RESP MULTI block with one SET and no EXEC — simulates a crash mid-transaction. */
    private byte[] buildIncompleteMultiTransaction(String key, String value) {
        String resp = "*1\r\n$5\r\nMULTI\r\n"
                + "*3\r\n$3\r\nSET\r\n"
                + "$" + key.length() + "\r\n" + key + "\r\n"
                + "$" + value.length() + "\r\n" + value + "\r\n";
        return resp.getBytes();
    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1 << 20, 2, Long.MAX_VALUE, 60 * 1000);
    }

    @Override
    protected int getInitSleepMilli() {
        return 5000;
    }

    @Override
    protected void startRedis(RedisMeta redisMeta) throws IOException {
        if (redisMeta.equals(getRedisMaster())) {
            super.startRedis(redisMeta);
        } else {
            logger.info("[startRedis][skip non-master]{}", redisMeta);
        }
    }
}
