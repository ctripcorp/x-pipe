package com.ctrip.xpipe.redis.meta.server;



import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerContextTest extends AbstractMetaServerTest{
	
	private String xpipeConfig = "meta-test.xml";
	
	protected MetaServerConfig  config = new DefaultMetaServerConfig();

	private String zkAddress;

	@Before
	public void beforeAbstractMetaServerTest() throws Exception{
		arrangeTaskStart(false);
		
		int zkPort = randomPort();
		if(isStartZk()){
			startZk(zkPort);
		}
		
		zkAddress = String.format("localhost:%d", zkPort);
		getZkClient();//set zk address and start
	}
	
	protected boolean isStartZk() {
		return true;
	}

	@Override
	protected void setProperties() {
		super.setProperties();
		System.setProperty(DefaultDcMetaCache.MEMORY_META_SERVER_DAO_KEY, "metaserver--jq.xml");
		System.setProperty("TOTAL_SLOTS", "16");
	}
	
	
	protected void arrangeTaskStart(boolean isStart) {
		System.setProperty(ArrangeTaskExecutor.ARRANGE_TASK_EXECUTOR_START, String.valueOf(isStart));
	}


	@Override
	protected ApplicationContext createSpringContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getEnvironment().setActiveProfiles("test");
		context.register(MetaServerContextConfig.class);
		context.refresh();
		return context;
	}
	
	
	public ZkClient getZkClient() throws Exception {
		
		try{
			ZkClient zkClient = getBean(ZkClient.class);
			((DefaultZkClient)zkClient).setZkAddress(zkAddress);
			LifecycleHelper.initializeIfPossible(zkClient);
			LifecycleHelper.startIfPossible(zkClient);
			return zkClient;
		}catch(Exception e){
			logger.info(e.getMessage());
			return null;
		}
	}

	public CuratorFramework getCurator() throws Exception {
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

	protected ClusterServers<?> getClusterServers() throws Exception {
		return getBean(ClusterServers.class);
	}

	protected  SlotManager getSlotManager() throws Exception {
		return getBean(SlotManager.class);
	}
	
	public String getZkAddress() {
		return zkAddress;
	}
}
