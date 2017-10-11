package com.ctrip.xpipe.redis.meta.server.cluster.impl;


import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


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
		sleep(300);
		
		servers = (AbstractClusterServers<?>) getBean(ClusterServers.class);
	}
	
	@Test
	public void testRestart() throws Exception{
		
		CurrentClusterServer currentClusterServer = getCurrentClusterServer();

		Assert.assertEquals(1, servers.allClusterServers().size());
		
		logger.info("[testRestart][stop]");
		currentClusterServer.stop();
		sleep(300);
		Assert.assertEquals(0, servers.allClusterServers().size());
		
		logger.info("[testRestart][start again]");
		currentClusterServer.start();
		sleep(300);
		Assert.assertEquals(1, servers.allClusterServers().size());
	}

	@Test
	public void testStartServerWithDifferentConfig() throws Exception{
		
		
		Assert.assertEquals(1, servers.allClusterServers().size());

		UnitTestServerConfig config20 = new UnitTestServerConfig(2, randomPort());
		logger.info(remarkableMessage("[testServers][start server2]{}"), config20);
		@SuppressWarnings("unused")
		CurrentClusterServer current20 = createAndStart(config20);
		sleep(500);
		
		Assert.assertEquals(2, servers.allClusterServers().size());


		UnitTestServerConfig config21 = new UnitTestServerConfig(2, config20.getMetaServerPort() + 1);
		try{
			logger.info(remarkableMessage("[testServers][start server2 with another port again]{}"), config21);
			@SuppressWarnings("unused")
			CurrentClusterServer current21 = createAndStart(config21);
			Assert.fail();
		}catch(IllegalStateException e){
			//pass
		}
	}

	@SuppressWarnings("unused")
	@Test
	public void testStartServerWithSameConfig() throws Exception{
		
		Assert.assertEquals(1, servers.allClusterServers().size());

		logger.info(remarkableMessage("[testServers][start server2]"));
		UnitTestServerConfig config2 = new UnitTestServerConfig(2, randomPort());
		CurrentClusterServer current2 = createAndStart(config2);
		sleep(500);
		Assert.assertEquals(2, servers.allClusterServers().size());

		try{
			logger.info(remarkableMessage("[testServers][start server2 again]"));
			CurrentClusterServer current2Copy = createAndStart(config2);
			Assert.fail();
		}catch(IllegalStateException e){
			
		}
		sleep(500);
		Assert.assertEquals(2, servers.allClusterServers().size());
	}

}
