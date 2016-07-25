package com.ctrip.xpipe.redis.meta.server;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerTest extends AbstractRedisTest{
	
	private String xpipeConfig = "meta-test.xml";
	
	private ZkClient zkClient = new DefaultZkClient();
	protected MetaServerConfig  config = new DefaultMetaServerConfig(); 


	@Before
	public void beforeAbstractMetaServerTest() throws Exception{
		startZkAndPrePareZkClient();
	}

	
	protected void startZkAndPrePareZkClient() throws Exception {
		
		int zkPort = randomPort();
		startZk(zkPort);
		
		zkClient.setZkAddress("localhost:" + zkPort);
		zkClient.initialize();
		zkClient.start();
	}
	
	public ZkClient getZkClient() {
		return zkClient;
	}

	public CuratorFramework getCurator() {
		return zkClient.get();
	}

	
	public CurrentClusterServer createAndStart(MetaServerConfig metaServerConfig) throws Exception{
		
		DefaultCurrentClusterServer current = new DefaultCurrentClusterServer();
		current.setZkClient(zkClient);
		current.setConfig(metaServerConfig);
		current.initialize();
		current.start();
	
		return current;
	}

	
	@Override
	protected String getXpipeMetaConfigFile() {
		return xpipeConfig;
	}
	
}
