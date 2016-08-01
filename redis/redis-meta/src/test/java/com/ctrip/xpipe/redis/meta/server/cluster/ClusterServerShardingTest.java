package com.ctrip.xpipe.redis.meta.server.cluster;

import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Aug 1, 2016
 */
public class ClusterServerShardingTest extends AbstractMetaServerClusterTest{
	
	private int serverCount = 3; 
	
	@Before
	public void beforeClusterServerShardingTest(){
		
	}

	@Test
	public void testInit() throws Exception{
		
		createMetaServers(serverCount);
		
		
		waitForAnyKeyToExit();
	}

}
