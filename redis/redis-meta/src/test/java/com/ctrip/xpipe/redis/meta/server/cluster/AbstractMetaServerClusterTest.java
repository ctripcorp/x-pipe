package com.ctrip.xpipe.redis.meta.server.cluster;



import java.util.LinkedList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.springframework.context.ApplicationContext;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class AbstractMetaServerClusterTest extends AbstractMetaServerTest{
	
	private int zkPort = portUsable(defaultZkPort());
	
	@Before
	public void beforeAbstractMetaServerClusterTest(){
		
		startZk(zkPort);
	}
	
	protected CuratorFramework getCuratorFramework() throws Exception{
		return getCuratorFramework(zkPort);
	}

	protected CuratorFramework getCuratorFramework(int zkPort) throws Exception{
		
		ZkClient client = new DefaultZkClient();
		client.setZkAddress(String.format("localhost:%d", zkPort));
		client.initialize();
		client.start();
		return client.get();
	}

	@Override
	protected ApplicationContext createSpringContext() {
		return null;
	}
	
	protected void createMetaServers(int serverCount) throws Exception{
		
		
		for(int i=0 ; i<serverCount ; i++){
			
			int port = portUsable(defaultMetaServerPort());
			TestMetaServer testMetaServer = new TestMetaServer(i + 1, port, zkPort);
			testMetaServer.initialize();
			testMetaServer.start();
			add(testMetaServer);
		}
	}
	
	public List<TestMetaServer> getServers() {
		return new LinkedList<>(getRegistry().getComponents(TestMetaServer.class).values());
	}
	
	public TestMetaServer getLeader(){
		
		for(TestMetaServer server : getServers()){
			if(server.isLeader()){
				return server;
			}
		}
		return null;
	}

	public TestMetaServer getRandomNotLeader(){
		
		for(TestMetaServer server : getServers()){
			if(!server.isLeader()){
				return server;
			}
		}
		return null;
	}

}
