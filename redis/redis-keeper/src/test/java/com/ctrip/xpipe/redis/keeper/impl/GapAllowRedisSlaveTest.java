package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GapAllowRedisSlaveTest extends AbstractRedisKeeperTest {

    private static final String MASTER_UUID = "22062162b08f6be03373f7e31f9a632d7404d31f";
    private static final String REPL_ID = "8fbe6e5e42a1ba4fe08f0ae2bd462db9e34502ba";

    @Mock
    private Channel channel;

    @Mock
    private RedisKeeperServer redisKeeperServer;

    @Mock
    private KeeperRepl keeperRepl;

    private TestableGapAllowRedisSlave redisSlave;

    @Before
    public void beforeGapAllowRedisSlaveTest() {
        when(channel.closeFuture()).thenReturn(new DefaultChannelPromise(channel));
        when(channel.remoteAddress()).thenReturn(localhostInetAdress(randomPort()));
        when(redisKeeperServer.getKeeperRepl()).thenReturn(keeperRepl);

        RedisClient redisClient = new DefaultRedisClient(channel, redisKeeperServer);
        redisSlave = new TestableGapAllowRedisSlave(redisClient);
    }

    @Test
    public void testXfullResyncLostEmptyWhenRdbExecutedCoversKeeperLost() {
        GtidSet keeperLost = new GtidSet(MASTER_UUID + ":1056716181-1056722380");
        GtidSet rdbExecuted = new GtidSet(MASTER_UUID + ":1-1056722380");

        String resp = buildXfullResyncMark(keeperLost, rdbExecuted);

        Assert.assertTrue(resp.contains("GTID.LOST \"\""));
    }

    @Test
    public void testXfullResyncLostEmptyWhenKeeperLostEmpty() {
        GtidSet rdbExecuted = new GtidSet(MASTER_UUID + ":1-100");

        String resp = buildXfullResyncMark(new GtidSet(""), rdbExecuted);

        Assert.assertTrue(resp.contains("GTID.LOST \"\""));
    }

    @Test
    public void testXfullResyncLostUnchangedWhenNoOverlap() {
        GtidSet keeperLost = new GtidSet(MASTER_UUID + ":200-300");
        GtidSet rdbExecuted = new GtidSet(MASTER_UUID + ":1-100");

        String resp = buildXfullResyncMark(keeperLost, rdbExecuted);

        Assert.assertTrue(resp.contains("GTID.LOST " + MASTER_UUID + ":200-300"));
    }

    @Test
    public void testXfullResyncLostPartialOverlap() {
        GtidSet keeperLost = new GtidSet(MASTER_UUID + ":50-150");
        GtidSet rdbExecuted = new GtidSet(MASTER_UUID + ":1-100");

        String resp = buildXfullResyncMark(keeperLost, rdbExecuted);

        Assert.assertTrue(resp.contains("GTID.LOST " + MASTER_UUID + ":101-150"));
    }

    @Test
    public void testXfullResyncLostUsesKeeperLostWhenRdbExecutedNotSet() {
        GtidSet keeperLost = new GtidSet(MASTER_UUID + ":200-300");
        when(keeperRepl.currentStage()).thenReturn(xsyncStage(keeperLost));

        String resp = redisSlave.exposeBuildMark(new BacklogOffsetReplicationProgress(0L));

        Assert.assertTrue(resp.contains("GTID.LOST " + MASTER_UUID + ":200-300"));
    }

    private String buildXfullResyncMark(GtidSet keeperLost, GtidSet rdbGtidExecuted) {
        when(keeperRepl.currentStage()).thenReturn(xsyncStage(keeperLost));
        BacklogOffsetReplicationProgress progress = new BacklogOffsetReplicationProgress(0L);
        progress.setRdbGtidExecuted(rdbGtidExecuted);
        return redisSlave.exposeBuildMark(progress);
    }

    private ReplStage xsyncStage(GtidSet keeperLost) {
        return new ReplStage(REPL_ID, 1000L, 0L, MASTER_UUID, keeperLost, new GtidSet(""));
    }

    private static class TestableGapAllowRedisSlave extends GapAllowRedisSlave {

        TestableGapAllowRedisSlave(RedisClient redisClient) {
            super(redisClient, null);
        }

        String exposeBuildMark(ReplicationProgress<?> rdbProgress) {
            return buildMarkBeforeFsync(rdbProgress);
        }
    }
}
