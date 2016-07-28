package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class AbstractMetaServerClusterTest extends AbstractMetaServerTest{
	
	private List<ApplicationContext>  servers = new LinkedList<>(); 
	
	@Before
	public void beforeAbstractMetaServerClusterTest(){
	}
	
	
	public void initMetaCluster(int size) throws Exception{
		
		int serverIndex =0;
		for(int i=0;i<SlotManager.TOTAL_SLOTS;i++){
			int serverId = (serverIndex++)%size + 1;
			SlotInfo info = new SlotInfo(serverId);
			getZkClient().get().create().creatingParentsIfNeeded().forPath(MetaZkConfig.getMetaServerSlotsPath() + "/" + i, Codec.DEFAULT.encodeAsBytes(info));
		}
		
	}

	@Override
	protected ApplicationContext createSpringContext() {
		return null;
	}
	
	protected void createMetaServers(int serverCount) throws Exception{
		
		initMetaCluster(serverCount);
		
		for(int i=0;i<serverCount;i++){
			
			int port = randomPort();
			ApplicationContext applicationContext = new AnnotationConfigApplicationContext(MetaServerContextConfig.class);
			
			
			DefaultZkClient client = applicationContext.getBean(DefaultZkClient.class);
			client.setZkAddress(getZkAddress());

			DefaultMetaServerConfig config = applicationContext.getBean(DefaultMetaServerConfig.class);
			config.setZkConnectionString(getZkAddress());
			config.setDefaultMetaServerId(i + 1);
			config.setDefaultServerPort(port);
			
			
			SpringComponentLifecycleManager lifeCycleManager = applicationContext.getBean(SpringComponentLifecycleManager.class);
			lifeCycleManager.startAll();
			
			servers.add(applicationContext);
		}
	}
	
	
	
	
}
