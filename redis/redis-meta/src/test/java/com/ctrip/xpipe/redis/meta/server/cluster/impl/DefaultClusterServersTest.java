package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;


/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class DefaultClusterServersTest extends AbstractMetaServerTest{
	
	private DefaultClusterServers servers;
	private MetaServerConfig  currentConfig;
	
	@Before
	public void beforeDefaultClusterServersTest() throws Exception{
		
		initRegistry();
		startRegistry();
		
		currentConfig = new DefaultMetaServerConfig();
		servers = new DefaultClusterServers();
		servers.setCurrentServer(createAndStart(currentConfig));
		servers.setMetaServerConfig(currentConfig);
		servers.setRemoteClusterServerFactory(new DefaultRemoteClusterSeverFactory());
		servers.setZkClient(getZkClient());
		
	}
	
	@Test
	public void testServers() throws Exception{
		
		servers.initialize();
		servers.start();
		
		Assert.assertEquals(1, servers.allClusterServers().size());
		
		DefaultMetaServerConfig config2 = new DefaultMetaServerConfig();
		config2.setDefaultMetaServerId(2);
		CurrentClusterServer current2 = createAndStart(config2);
		
		sleep(100);
		Assert.assertEquals(2, servers.allClusterServers().size());
		current2 = createAndStart(config2);
		
		Assert.assertEquals(2, servers.allClusterServers().size());
		current2.stop();
		sleep(100);
		Assert.assertEquals(1, servers.allClusterServers().size());
}

}
