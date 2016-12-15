package simpletest;

import org.junit.Test;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author shyin
 *
 * Dec 15, 2016
 */
public class FakeRedisServer extends AbstractRedisTest{
	@Test
	public void startTestRedis6379() throws Exception{
		startFakeRedisServer(6379);
		waitForAnyKeyToExit();
	}
	
	@Test
	public void startTestRedis6359() throws Exception{
		startFakeRedisServer(6359);
		waitForAnyKeyToExit();
	}
	
	@Test
	public void startTestRedis6369() throws Exception{
		startFakeRedisServer(6369);
		waitForAnyKeyToExit();
	}
}
