package com.ctrip.xpipe.redis.meta.server;


import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.dao.memory.MemoryMetaServerDao;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerTest extends AbstractRedisTest{
	
	private String xpipeConfig = "meta-test.xml";
	
	protected MetaServerConfig  config = new DefaultMetaServerConfig();

	private String zkAddress;

	@Before
	public void beforeAbstractMetaServerTest() throws Exception{
		
		System.setProperty(MemoryMetaServerDao.MEMORY_META_SERVER_DAO_KEY, "metaserver--jq.xml");
		
		int zkPort = randomPort();
		startZk(zkPort);
		
		zkAddress = String.format("localhost:%d", zkPort);
		((DefaultZkClient)getZkClient()).setZkAddress(zkAddress);
		
	}
	
	
	@Override
	protected ApplicationContext createSpringContext() {
		return new AnnotationConfigApplicationContext(MetaServerContextConfig.class);
	}
	
	
	public ZkClient getZkClient() {
		return getBean(ZkClient.class);
	}

	public CuratorFramework getCurator() {
		return getZkClient().get();
	}

	
	public CurrentClusterServer createAndStart(MetaServerConfig metaServerConfig) throws Exception{
		
		DefaultCurrentClusterServer current = new DefaultCurrentClusterServer();
		current.setZkClient(getZkClient());
		current.setConfig(metaServerConfig);
		current.initialize();
		current.start();
	
		return current;
	}

	
	@Override
	protected String getXpipeMetaConfigFile() {
		return xpipeConfig;
	}

	protected ClusterServers getClusterServers() throws Exception {
		return getBean(ClusterServers.class);
	}

	protected  SlotManager getSlotManager() throws Exception {
		return getBean(SlotManager.class);
	}
	
	public String getZkAddress() {
		return zkAddress;
	}
}
