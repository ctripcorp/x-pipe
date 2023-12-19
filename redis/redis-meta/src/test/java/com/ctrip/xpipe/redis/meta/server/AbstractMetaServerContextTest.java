package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
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
import com.ctrip.xpipe.zk.impl.TestZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerContextTest extends AbstractMetaServerTest{
	
	private String xpipeConfig = "metaserver--jq.xml";
	
	protected MetaServerConfig  config = new DefaultMetaServerConfig();

	private String zkAddress;
	
	private String dc = FoundationService.DEFAULT.getDataCenter();

	@Before
	public void beforeAbstractMetaServerContextTest() throws Exception{
		arrangeTaskStart(false);
		
		int zkPort = getTestZkPort();
				
		if(isStartZk()){
			startZk(zkPort);
		}
		
		zkAddress = String.format("127.0.0.1:%d", zkPort);
		try{
			TestZkClient client = getBean(TestZkClient.class);
			client.setZkAddress(zkAddress);
		}catch(IllegalStateException e){
			//not found, if not
		}
	}
	
	protected boolean isStartZk() {
		return true;
	}

	
	@Override
	protected void doBeforeAbstractTest() throws Exception {
		super.doBeforeAbstractTest();
		System.setProperty(DefaultDcMetaCache.MEMORY_META_SERVER_DAO_KEY, xpipeConfig);
		System.setProperty("TOTAL_SLOTS", "16");
	}

	protected void arrangeTaskStart(boolean isStart) {
		System.setProperty(ArrangeTaskExecutor.ARRANGE_TASK_EXECUTOR_START, String.valueOf(isStart));
	}


	@Override
	protected ConfigurableApplicationContext createSpringContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(MetaServerContextConfig.class);
		context.refresh();
		return context;
	}
	
	protected CurrentClusterServer getCurrentClusterServer(){
		
		return getBean(CurrentClusterServer.class);
	}
	
	
	public ZkClient getZkClient() throws Exception {
		
		Map<String, ZkClient> result = getRegistry().getComponents(ZkClient.class);
		if(result.size() > 0){
			return (ZkClient) result.values().toArray()[0];
		}
		throw new IllegalStateException("non zk client found");
	}

	public ZkClient createZkClient() throws Exception {
		
		try{
			TestZkClient zkClient = new TestZkClient();
			zkClient.setZkAddress(zkAddress);
			zkClient.initialize();
			zkClient.start();
			add(zkClient);
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
		current.setZkClient(createZkClient());
		current.setConfig(metaServerConfig);
		current.initialize();
		current.start();
		add(current);
		return current;
	}

	protected ClusterMeta randomClusterMeta() {
		
		DcMeta dcMeta = getDcMeta(dc);
		ClusterMeta clusterMeta = (ClusterMeta) dcMeta.getClusters().values().toArray()[0];
		return MetaCloneFacade.INSTANCE.clone(clusterMeta);
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
