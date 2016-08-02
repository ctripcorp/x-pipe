package com.ctrip.xpipe.redis.meta.server.cluster;



import java.util.LinkedList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.springframework.context.ApplicationContext;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class AbstractMetaServerClusterTest extends AbstractMetaServerTest{
	
	private List<TestAppServer>  servers = new LinkedList<>();
	private int zkPort = randomPort();
	
	@Before
	public void beforeAbstractMetaServerClusterTest(){
		
		startZk(zkPort);
	}
	
	protected CuratorFramework getCuratorFramework() throws Exception{
		
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
			
			int port = randomPort();
			TestAppServer testAppServer = new TestAppServer(i + 1, port, zkPort);
			testAppServer.start();
			servers.add(testAppServer);
		}
	}
	
	public List<TestAppServer> getServers() {
		return servers;
	}
	
	public TestAppServer getLeader(){
		
		for(TestAppServer server : servers){
			if(server.isLeader()){
				return server;
			}
		}
		return null;
	}

	public TestAppServer getRandomNotLeader(){
		
		for(TestAppServer server : servers){
			if(!server.isLeader()){
				return server;
			}
		}
		return null;
	}

}
