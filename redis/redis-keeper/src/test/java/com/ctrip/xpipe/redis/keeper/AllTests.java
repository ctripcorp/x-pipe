package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.redis.keeper.applier.AllApplierTests;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfigTest;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerServiceTest;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManagerTest;
import com.ctrip.xpipe.redis.keeper.handler.applier.ApplierCommandHandlerTest;
import com.ctrip.xpipe.redis.keeper.handler.keeper.*;
import com.ctrip.xpipe.redis.keeper.health.DiskHealthCheckerTest;
import com.ctrip.xpipe.redis.keeper.impl.*;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.*;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync.XsyncForKeeperAndKeeperTest;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync.XsyncForKeeperSlaveTest;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync.XsyncForKeeperTest;
import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReasonTest;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperStatsTest;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultMasterStatsTest;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultReplicationStoreStatsTest;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.GapAllowedSyncTest;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.PsyncTest;
import com.ctrip.xpipe.redis.keeper.ratelimit.CompositeLeakyBucketTest;
import com.ctrip.xpipe.redis.keeper.ratelimit.DefaultLeakyBucketTest;
import com.ctrip.xpipe.redis.keeper.ratelimit.LeakyBucketBasedMasterReplicationListenerTest;
import com.ctrip.xpipe.redis.keeper.ratelimit.RateLimitTest;
import com.ctrip.xpipe.redis.keeper.ratelimit.impl.FixSyncRateManagerTest;
import com.ctrip.xpipe.redis.keeper.store.*;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidCmdOneSegmentReaderTest;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetStreamCommandReaderTest;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.DefaultIndexStoreTest;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.StreamCommandReaderTest;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStoreTest;
import com.ctrip.xpipe.redis.keeper.store.meta.TestAbstractMetaStoreTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 * <p>
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
        AbstractRdbDumperTest.class,
        DefaultRedisKeeperServerConnectToFakeRedisTest.class,
        FakeRedisHalfRdbServerFail.class,
        PsyncTest.class,
        GapAllowedSyncTest.class,
        DefaultRedisClientTest.class,
        CommandHandlerManagerTest.class,
        RedisKeeperServerStateBackupTest.class,
        RedisKeeperServerStateActiveTest.class,
        RedisKeeperServerStateUnknownTest.class,
        DefaultRedisMasterReplicationTest.class,
        RdbonlyRedisMasterReplicationTest.class,
        GapAllowedRdbonlyRedisMasterReplicationTest.class,
        RedisMasterNewRdbDumperTest.class,
        StateBackupDeadlockTest.class,
        KeeperContainerServiceTest.class,
        DefaultReplicationStoreManagerTest.class,
        DefaultRedisKeeperServerTest.class,
        DefaultReplicationStoreTest.class,
        GapAllowedReplicationStoreTest.class,
        DefaultRdbStoreTest.class,
        DefaultRdbStoreEofMarkTest.class,
        DefaultCommandStoreTest.class,
        DefaultRedisSlaveTest.class,
        RoleCommandHandlerTest.class,
        DefaultKeeperConfigTest.class,
        FakeRedisExceptionTest.class,
        FakeRedisRdbDumperTest.class,
        FakeRedisRdbDumpLong.class,
        SlaveOfCommandHandlerTest.class,
        KeeperCommandHandlerTest.class,
        InfoHandlerTest.class,
        ConfigHandlerTest.class,
        ApplierCommandHandlerTest.class,
        FakeRedisRdbOnlyDumpTest.class,

        DefaultKeeperStatsTest.class,
        DefaultLeakyBucketTest.class,
        CompositeLeakyBucketTest.class,
        RateLimitTest.class,
        LeakyBucketBasedMasterReplicationListenerTest.class,
        DefaultReplicationStoreStatsTest.class,
        DefaultMetaStoreTest.class,
        TestAbstractMetaStoreTest.class,
        PsyncFailReasonTest.class,
        DefaultMasterStatsTest.class,
        PsyncForKeeperTest.class,
        PsyncKeeperServerStateObserverTest.class,

        GtidCmdOneSegmentReaderTest.class,
        GtidSetStreamCommandReaderTest.class,

        DefaultIndexStoreTest.class,
        StreamCommandReaderTest.class,

        DiskHealthCheckerTest.class,

        RordbReplicationSupportTest.class,

        FixSyncRateManagerTest.class,

        GapAllowSyncHandlerTest.class,
        GapAllowXSyncHandlerTest.class,

        XsyncForKeeperAndKeeperTest.class,
        XsyncForKeeperSlaveTest.class,
        XsyncForKeeperTest.class,
        AllApplierTests.class,
})
public class AllTests {

}
