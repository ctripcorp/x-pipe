package com.ctrip.xpipe.redis.meta.server;
import org.junit.Before;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerTest extends AbstractRedisTest{
	
	private String xpipeConfig = "meta-test.xml";

	@Before
	public void beforeAbstractMetaServerTest(){
	}

	
	@Override
	protected String getXpipeMetaConfigFile() {
		return xpipeConfig;
	}
	
}
