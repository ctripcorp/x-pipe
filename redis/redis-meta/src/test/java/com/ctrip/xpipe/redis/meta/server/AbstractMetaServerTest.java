package com.ctrip.xpipe.redis.meta.server;



import org.junit.After;
import org.junit.Before;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.testutils.MemoryPrinter;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerTest extends AbstractRedisTest{
	
	private String xpipeConfig = "meta-test.xml";
	private String dc = "jq", clusterId = "cluster1", shardId = "shard1";
	
	protected MetaServerConfig  config = new DefaultMetaServerConfig();
	
	private MemoryPrinter memoryPrinter;
	
	@Before
	public void beforeAbstractMetaServerTest() throws Exception{

		memoryPrinter = new MemoryPrinter(scheduled, 500);
		memoryPrinter.start();
	}

	@After
	public void afterAbstractMetaServerTest() throws Exception{
		memoryPrinter.stop();
	}

	
	@Override
	protected String getXpipeMetaConfigFile() {
		return xpipeConfig;
	}

	
	public String getDc() {
		return dc;
	}
	
	public String [] getDcs(){
		return new String[]{"jq", "fq"};
	}
	
	public String getClusterId() {
		return clusterId;
	}
	
	public String getShardId() {
		return shardId;
	}
	
}
