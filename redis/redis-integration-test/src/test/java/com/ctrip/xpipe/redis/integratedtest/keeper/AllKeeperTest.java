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
	KeeperSingleDc.class, 
	KeeperMultiDc.class, 
	KeeperSingleDcRestart.class, 
	KeeperSingleDcSlaveof.class,
	KeeperSingleDcWipeOutData.class
	})
public class AllKeeperTest {

}
