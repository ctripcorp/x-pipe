package com.ctrip.xpipe.redis.console;


import com.ctrip.xpipe.redis.console.migration.MultiShardMigrationTest;
import com.ctrip.xpipe.redis.console.migration.SingleShardMigrationTest;
import com.ctrip.xpipe.redis.console.migration.model.DefaultMigrationClusterTest;
import com.ctrip.xpipe.redis.console.migration.model.DefaultMigrationShardTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationPublishStatTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatusTest;

import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.console.dal.ConcurrentDalTransactionTest;
import com.ctrip.xpipe.redis.console.dal.DalTransactionManagerTest;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifierTest;
import com.ctrip.xpipe.redis.console.notifier.MetaNotifyTaskTest;
import com.ctrip.xpipe.redis.console.service.MetaServiceTest;

import org.junit.runner.RunWith;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@RunWith(Suite.class)
@SuiteClasses({
	MigrationStatusTest.class,
	ConcurrentDalTransactionTest.class,
	DalTransactionManagerTest.class,
	ClusterMetaModifiedNotifierTest.class,
	MetaServiceTest.class,
	ClusterMetaModifiedNotifierTest.class,
	MetaNotifyTaskTest.class,
	DefaultMigrationClusterTest.class,
	DefaultMigrationShardTest.class,
	MigrationStatTest.class,
	MigrationPublishStatTest.class,
	SingleShardMigrationTest.class,
	MultiShardMigrationTest.class
})
public class AllTests {

}
