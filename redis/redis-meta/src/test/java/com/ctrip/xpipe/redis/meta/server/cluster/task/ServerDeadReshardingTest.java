package com.ctrip.xpipe.redis.meta.server.cluster.task;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.meta.server.cluster.AbstractMetaServerClusterTest;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class ServerDeadReshardingTest extends AbstractMetaServerClusterTest{
	
	private int clusterSize = 5;
	private ClusterServer deadServer;
	
	@Before
	public void beforeServerDeadReshardingTest() throws Exception{
		
		initRegistry();
		startRegistry();
	}

	@Test
	public void test() throws Exception{

		
		SlotManager slotManager = getSlotManager();
		ClusterServers servers = getClusterServers();
		deadServer = (ClusterServer) servers.allClusterServers().toArray()[0];
		
		waitForAnyKeyToExit();
	}

}
