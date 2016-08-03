package com.ctrip.xpipe.redis.meta.server;



import org.junit.Before;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerTest extends AbstractRedisTest{
	
	private String xpipeConfig = "meta-test.xml";
	
	protected MetaServerConfig  config = new DefaultMetaServerConfig();

	@Before
	public void beforeAbstractMetaServerTest() throws Exception{
	}
	
	
	@Override
	protected String getXpipeMetaConfigFile() {
		return xpipeConfig;
	}

}
