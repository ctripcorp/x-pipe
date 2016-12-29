package com.ctrip.xpipe.redis.keeper;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServerTest;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisSlaveTest;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActiveTest;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackupTest;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateUnknownTest;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.DefaultRedisKeeperServerConnectToFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.FakeRedisExceptionTest;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.FakeRedisRdbDumpLong;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.FakeRedisRdbDumperTest;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.PsyncTest;
import com.ctrip.xpipe.redis.keeper.store.DefaultCommandStoreTest;
import com.ctrip.xpipe.redis.keeper.store.DefaultRdbStoreTest;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManagerTest;
import com.ctrip.xpipe.redis.keeper.store.meta.BackupMetaStoreTest;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfigTest;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerServiceTest;
import com.ctrip.xpipe.redis.keeper.handler.RoleCommandHandlerTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	DefaultRedisKeeperServerConnectToFakeRedisTest.class,
	PsyncTest.class,
	RedisKeeperServerStateBackupTest.class,
	RedisKeeperServerStateActiveTest.class,
	RedisKeeperServerStateUnknownTest.class,
	KeeperContainerServiceTest.class,
	DefaultReplicationStoreManagerTest.class,
	DefaultRedisKeeperServerTest.class,
	DefaultRdbStoreTest.class,
	DefaultCommandStoreTest.class,
	DefaultRedisSlaveTest.class,
	RoleCommandHandlerTest.class,
	DefaultKeeperConfigTest.class,
	FakeRedisExceptionTest.class, 
	FakeRedisRdbDumperTest.class,
	FakeRedisRdbDumpLong.class,
	BackupMetaStoreTest.class
})
public class AllTests {

}
