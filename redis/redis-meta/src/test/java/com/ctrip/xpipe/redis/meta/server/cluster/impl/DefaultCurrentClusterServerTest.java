package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class DefaultCurrentClusterServerTest extends AbstractMetaServerContextTest{
	
	private DefaultCurrentClusterServer currentServer;
	
	@Before
	public void beforeDefaultCurrentClusterServerTest() throws Exception{
		
		currentServer = new DefaultCurrentClusterServer();
		currentServer.setConfig(config);
		currentServer.setZkClient(createZkClient());
		
		add(currentServer);
	}
	
	

	@Test
	public void testStartStop() throws Exception{
		
		currentServer.initialize();
		currentServer.start();
		sleep(150);
		logger.info("[testStartStop][check exists]");
		Stat stat = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNotNull(stat);
		
		currentServer.stop();
		logger.info("[testStartStop][check not exists]");
		stat = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNull(stat);
		
	}

	@Test(expected = IllegalStateException.class)
	public void testRestartHard() throws Exception{

		currentServer.initialize();
		currentServer.start();
		
		sleep(100);
		Stat stat1 = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNotNull(stat1);
		
		DefaultCurrentClusterServer newServer = new DefaultCurrentClusterServer();
		newServer.setConfig(config);
		newServer.setZkClient(createZkClient());
		newServer.initialize();
		newServer.start();

		Stat stat2 = getCurator().checkExists().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + config.getMetaServerId());
		Assert.assertNotNull(stat2);
		Assert.assertNotEquals(stat1, stat2);
		
	}
	
}
