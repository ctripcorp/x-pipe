package com.ctrip.xpipe.redis.integratedtest;




import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import com.ctrip.xpipe.redis.integratedtest.multidc.MultiDcTests;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:09:41 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	MultiDcTests.class
})
public class AllTests {

}
