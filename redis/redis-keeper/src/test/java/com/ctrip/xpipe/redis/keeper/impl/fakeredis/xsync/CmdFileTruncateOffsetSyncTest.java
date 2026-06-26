package com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Integration test: after crash with incomplete transaction in cmd file,
 * keeper must report the correctly-truncated offset to master on psync,
 * so that master sends data aligned to a command boundary.
 *
 * Bug reproduced: buildIndexFromCmdFile truncated the cmd file on disk,
 * but CommandFileContext.fileLength (AtomicLong cache) was NOT updated.
 * locateTailOfCmd() returned the stale (pre-truncation) totalLength,
 * master responded +CONTINUE from that offset, and keeper's parser received
 * data starting inside a binary value → NumberFormatException: "$10".
 */
public class CmdFileTruncateOffsetSyncTest extends AbstractFakeRedisTest {

    private static final String REPLID = "aabbccdd11223344556677889900aabbccdd1122";
    private static final String UUID40 = REPLID;

    @Override
    protected String getProto() {
        return "xsync";
    }

    /**
     * Builds a GTID SET command string.
     */
    private String gtidSetCmd(String uuid, int gno, String key, String value) {
        // *6\r\n$4\r\nGTID\r\n$<len>\r\n<uuid:gno>\r\n$1\r\n0\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n
        String gtid = uuid + ":" + gno;
        return "*6\r\n"
                + "$4\r\nGTID\r\n"
                + "$" + gtid.length() + "\r\n" + gtid + "\r\n"
                + "$1\r\n0\r\n"
                + "$3\r\nSET\r\n"
                + "$" + key.length() + "\r\n" + key + "\r\n"
                + "$" + value.length() + "\r\n" + value + "\r\n";
    }

    /**
     * Builds an incomplete MULTI transaction: MULTI + SET, no EXEC.
     */
    private String incompleteMultiCmd(String key, String value) {
        return "*1\r\n$5\r\nMULTI\r\n"
                + "*3\r\n$3\r\nSET\r\n"
                + "$" + key.length() + "\r\n" + key + "\r\n"
                + "$" + value.length() + "\r\n" + value + "\r\n";
    }

    /**
     * Core scenario:
     * 1. Keeper connects to fake-redis, receives N complete GTID commands.
     * 2. Simulate crash: append an incomplete MULTI transaction directly to the cmd file.
     * 3. Restart keeper (same data dir, simulating crash recovery).
     * 4. After init (recoverIndex + buildIndexFromCmdFile), verify:
     *    a. Cmd file was truncated back to the last complete command boundary.
     *    b. CommandWriter.totalLength() == cmd_file_start + truncated_cmd_file_size.
     *       (This is what master receives as psync offset — it must NOT include the garbage bytes.)
     *    c. GtidSet reflects only the complete commands.
     */
    @Test
    public void testTruncatedCmdFileOffsetSyncedToCommandWriter() throws Exception {
        // Step 1: start keeper, connect to fake-redis, write some complete commands
        RedisKeeperServer keeper = startRedisKeeperServerAndConnectToFakeRedis();
        waitRedisKeeperServerConnected(keeper);

        for (int i = 1; i <= 5; i++) {
            fakeRedisServer.propagate(gtidSetCmd(UUID40, i, "key" + i, "val" + i));
        }
        Thread.sleep(500);

        // Verify keeper received all 5 GTIDs
        GtidSet gtidSetBefore = keeper.getReplicationStore().getGtidSet().getKey();
        Assert.assertTrue("should have gno 1-5",
                gtidSetBefore.toString().contains(UUID40 + ":1-5"));

        // Step 2: find the current cmd file and append incomplete MULTI directly to disk
        AbstractCommandStore cmdStore = (AbstractCommandStore) keeper.getReplicationStore().getCommandStore();
        CommandWriter cmdWriter = cmdStore.getCommandWriter();
        long totalLengthBeforeCrash = cmdWriter.totalLength();
        long fileLengthBeforeCrash = cmdWriter.fileLength();

        File cmdFile = cmdWriter.getFileContext().getCommandFile().getFile();
        String incompleteTx = incompleteMultiCmd("crashKey", "crashVal");
        try (RandomAccessFile raf = new RandomAccessFile(cmdFile, "rw")) {
            raf.seek(cmdFile.length());
            raf.write(incompleteTx.getBytes());
        }
        long fileLengthAfterCrash = cmdFile.length();
        Assert.assertTrue("incomplete tx should have grown the file",
                fileLengthAfterCrash > fileLengthBeforeCrash);

        // Step 3: simulate keeper restart — stop and reinitialize
        keeper.stop();
        keeper.dispose();

        RedisKeeperServer newKeeper = startRedisKeeperServer();
        // Do NOT connect to fake-redis yet — just verify the recovered state

        // Step 4a: cmd file must be truncated back to pre-crash length
        AbstractCommandStore newCmdStore = (AbstractCommandStore) newKeeper.getReplicationStore().getCommandStore();
        CommandWriter newCmdWriter = newCmdStore.getCommandWriter();
        File newCmdFile = newCmdWriter.getFileContext().getCommandFile().getFile();

        Assert.assertEquals(
                "cmd file must be truncated back to last complete command",
                fileLengthBeforeCrash, newCmdFile.length());

        // Step 4b: CommandWriter.totalLength() must reflect the truncated size
        long recoveredTotalLength = newCmdWriter.totalLength();
        Assert.assertEquals(
                "CommandWriter.totalLength() must equal start_offset + truncated_file_length " +
                "(stale cache causes psync offset to be too large → master sends data mid-payload)",
                totalLengthBeforeCrash, recoveredTotalLength);

        // Step 4c: GtidSet must only contain the 5 complete commands
        GtidSet recoveredGtidSet = newKeeper.getReplicationStore().getGtidSet().getKey();
        Assert.assertTrue("recovered gtidset must contain gno 1-5",
                recoveredGtidSet.toString().contains(UUID40 + ":1-5"));
        Assert.assertFalse("recovered gtidset must NOT contain gno 6+",
                recoveredGtidSet.contains(UUID40, 6));
    }

    /**
     * Variant: incomplete raw bytes (not even a full RESP frame), simulating a
     * partial write crash (e.g. JVM killed mid-write).
     *
     * buildIndexFromCmdFile detects remainBytes > 0 and truncates.
     * The same syncCommandWriterOffset path must fire.
     */
    @Test
    public void testPartialWriteCrash_OffsetSyncedToCommandWriter() throws Exception {
        RedisKeeperServer keeper = startRedisKeeperServerAndConnectToFakeRedis();
        waitRedisKeeperServerConnected(keeper);

        for (int i = 1; i <= 3; i++) {
            fakeRedisServer.propagate(gtidSetCmd(UUID40, i, "k" + i, "v" + i));
        }
        Thread.sleep(500);

        AbstractCommandStore cmdStore = (AbstractCommandStore) keeper.getReplicationStore().getCommandStore();
        CommandWriter cmdWriter = cmdStore.getCommandWriter();
        long totalLengthBeforeCrash = cmdWriter.totalLength();
        long fileLengthBeforeCrash = cmdWriter.fileLength();
        File cmdFile = cmdWriter.getFileContext().getCommandFile().getFile();

        // append a partial RESP frame (no \r\n terminator → parser's remainBuf will be non-zero)
        byte[] partialFrame = "*3\r\n$3\r\nSET\r\n$4\r\nkey".getBytes();
        try (RandomAccessFile raf = new RandomAccessFile(cmdFile, "rw")) {
            raf.seek(cmdFile.length());
            raf.write(partialFrame);
        }

        keeper.stop();
        keeper.dispose();

        RedisKeeperServer newKeeper = startRedisKeeperServer();

        AbstractCommandStore newCmdStore = (AbstractCommandStore) newKeeper.getReplicationStore().getCommandStore();
        CommandWriter newCmdWriter = newCmdStore.getCommandWriter();
        File newCmdFile = newCmdWriter.getFileContext().getCommandFile().getFile();

        Assert.assertEquals(
                "cmd file must be truncated to remove partial frame",
                fileLengthBeforeCrash, newCmdFile.length());

        Assert.assertEquals(
                "CommandWriter.totalLength() must equal pre-crash value after truncation",
                totalLengthBeforeCrash, newCmdWriter.totalLength());
    }
}
