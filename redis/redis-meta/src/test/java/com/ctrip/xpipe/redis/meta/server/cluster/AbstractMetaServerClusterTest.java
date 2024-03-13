package com.ctrip.xpipe.redis.meta.server.cluster;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class AbstractMetaServerClusterTest extends AbstractMetaServerTest{
	
	protected int zkPort = getTestZkPort();
	
	@Before
	public void beforeAbstractMetaServerClusterTest(){
		
		startZk(zkPort);
	}
	
	
	public int getZkPort() {
		return zkPort;
	}
	
	protected CuratorFramework getCuratorFramework() throws Exception{
		return getCuratorFramework(zkPort);
	}

	protected CuratorFramework getCuratorFramework(int zkPort) throws Exception{

		ZkConfig zkConfig = new DefaultZkConfig(String.format("localhost:%d", zkPort));
		ZkClient client = new DefaultZkClient(zkConfig);
		LifecycleHelper.initializeIfPossible(client);
		LifecycleHelper.startIfPossible(client);
		return client.get();
	}

	protected void createMetaServers(int serverCount) throws Exception{
		
		for(int i=0 ; i<serverCount ; i++){
			
			int port = portUsable(defaultMetaServerPort());
			TestMetaServer testMetaServer = createMetaServer(i+1, port, zkPort);
			add(testMetaServer);
		}
	}
	
	protected TestMetaServer createMetaServer(int index, int port, int zkPort) throws Exception {
		
		return createMetaServer(index, port, zkPort, TestMetaServer.DEFAULT_CONFIG_FILE);
	}

	protected TestMetaServer createMetaServer(int index, int port, int zkPort, String configFile) throws Exception {
		
		TestMetaServer testMetaServer = new TestMetaServer(index, port, zkPort, configFile);
		
		testMetaServer.initialize();
		testMetaServer.start();
		return testMetaServer;
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
