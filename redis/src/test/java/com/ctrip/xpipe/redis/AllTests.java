package com.ctrip.xpipe.redis;


import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActiveTest;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackupTest;
import com.ctrip.xpipe.redis.protocal.cmd.PsyncTest;
import com.ctrip.xpipe.redis.protocal.protocal.ArrayParserTest;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParserTest;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParserTest;
import com.ctrip.xpipe.redis.util.OffsetNotifierTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	PsyncTest.class,
	SimpleStringParserTest.class,
	BulkStringParserTest.class,
	ArrayParserTest.class,
	OffsetNotifierTest.class,
	RedisKeeperServerStateBackupTest.class,
	RedisKeeperServerStateActiveTest.class
})
public class AllTests {

}
