package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.BeforeClass;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public abstract class AbstractConsoleTest extends AbstractRedisTest{
	
	@BeforeClass
	public static void beforeAbstractConsoleTest(){
		System.setProperty(HealthChecker.ENABLED, "false");
	}
	
	
}
