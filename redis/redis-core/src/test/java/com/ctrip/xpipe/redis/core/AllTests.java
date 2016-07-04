package com.ctrip.xpipe.redis.core;




import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParserTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParserTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	ArrayParserTest.class,
	BulkStringParserTest.class,
	ArrayParserTest.class,
})
public class AllTests {

}
