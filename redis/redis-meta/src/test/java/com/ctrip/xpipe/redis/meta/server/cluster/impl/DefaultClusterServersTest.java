package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;


/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class DefaultClusterServersTest extends AbstractMetaServerContextTest{
	
	private AbstractClusterServers<?> servers;
	
	@Before
	public void beforeDefaultClusterServersTest() throws Exception{
		
		initRegistry();
		startRegistry();
		
		servers = (AbstractClusterServers<?>) getBean(ClusterServers.class);
	}
	
	@Test
	public void testServers() throws Exception{
		
		sleep(100);
		
		Assert.assertEquals(1, servers.allClusterServers().size());
		
		DefaultMetaServerConfig config2 = new DefaultMetaServerConfig();
		config2.setDefaultMetaServerId(2);
		CurrentClusterServer current2 = createAndStart(config2);
		
		sleep(500);
		Assert.assertEquals(2, servers.allClusterServers().size());
		current2 = createAndStart(config2);
		
		sleep(100);
		Assert.assertEquals(2, servers.allClusterServers().size());
		current2.stop();
		sleep(500);
		Assert.assertEquals(1, servers.allClusterServers().size());
}

}
