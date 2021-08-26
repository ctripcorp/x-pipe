package com.ctrip.xpipe.redis.integratedtest.keeper;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 *
 *         May 17, 2016 2:09:41 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
		KeeperPsync2.class,
		KeeperPsync2Continue.class,
		KeeperSingleDc.class,
		KeeperMultiDc.class,
		KeeperMultiDcChangePrimary.class,
		KeeperSingleDcRestart.class,
		KeeperSingleDcSlaveof.class,
		KeeperSingleDcWipeOutData.class,
		KeeperSingleDcEof.class,
		KeeperMultiDc.class,
		KeeperSingleDcWaitForOffset.class,
		XRedisXpipeCommandTest.class,
		XRedisPartialTest.class,
		TwoKeepers.class,
		KeeperCmdFileMissTest.class,
		KeeperRdbNotContinueTest.class,
		PartialSyncForKeeperTest.class
})
public class AllKeeperTest {
	
	/*
	 * before run test, you should 
	 * 1. start redis 2.8.19 at localhost, for testCase: 	KeeperSingleDcVersionTest
	 */

}
