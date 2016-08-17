package com.ctrip.xpipe.redis.integratedtest;





import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.integratedtest.full.multidc.AllMultiDcTests;
import com.ctrip.xpipe.redis.integratedtest.full.singledc.AllSingleDcTests;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:09:41 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	AllMultiDcTests.class,
	AllSingleDcTests.class
})
public class AllTesters {

}
