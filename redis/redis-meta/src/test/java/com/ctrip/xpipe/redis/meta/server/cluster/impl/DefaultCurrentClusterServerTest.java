package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;


/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class DefaultCurrentClusterServerTest extends AbstractMetaServerContextTest{
	
	DefaultCurrentClusterServer currentServer;
	
	@Before
	public void beforeDefaultCurrentClusterServerTest() throws Exception{
		
		
		currentServer = new DefaultCurrentClusterServer();
		currentServer.setConfig(config);
		
		currentServer.setZkClient(getZkClient());
	}
	
	

	@Test
	public void testStartStop() throws Exception{
		
		currentServer.initialize();
		currentServer.start();
		sleep(20);
		Stat stat = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNotNull(stat);
		
		currentServer.stop();
		stat = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNull(stat);
		
	}

	@Test(expected = IllegalStateException.class)
	public void testRestartHard() throws Exception{

		currentServer.initialize();
		currentServer.start();
		
		sleep(20);
		Stat stat1 = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNotNull(stat1);
		
		DefaultCurrentClusterServer newServer = new DefaultCurrentClusterServer();
		newServer.setConfig(config);
		newServer.setZkClient(getZkClient());
		newServer.initialize();
		newServer.start();

		Stat stat2 = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNotNull(stat2);
		Assert.assertNotEquals(stat1, stat2);
		
	}
	
}
