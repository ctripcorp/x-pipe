package com.ctrip.xpipe.redis.meta.server.cluster;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskExecutor;

/**
 * @author wenchao.meng
 *
 * Aug 1, 2016
 */
public class ClusterServerShardingTest extends AbstractMetaServerClusterTest{
	
	private int serverCount = 3; 
	
	@Before
	public void beforeClusterServerShardingTest(){
		System.setProperty(ArrangeTaskExecutor.ARRANGE_TASK_EXECUTOR_START, "true");
		
	}

	@Test
	public void testInit() throws Exception{
		
		createMetaServers(serverCount);
		
		sleep(1000);
		
		for(TestAppServer server : getServers()){
			ApplicationContext context = server.getContext();
			SlotManager slotManager = context.getBean(SlotManager.class);
			slotManager.refresh();

			logger.info("[testInit]{}", server);
			AssertBalance(slotManager);
		}
	}

	@Test
	public void testShutdownOther() throws Exception{

		createMetaServers(serverCount);
		sleep(1000);
		for(TestAppServer server : getServers()){
			if(!server.isLeader()){
				logger.info(remarkableMessage("[testShutdownOther][begin]{}"), server);
				server.stop();
				logger.info(remarkableMessage("[testShutdownOther][ end ]{}"), server);
				break;
			}
		}
		sleep(TestAppServer.getWaitforrestarttimemills() + 1000);
		TestAppServer leader = getLeader();
		SlotManager slotManager = leader.getContext().getBean(SlotManager.class);
		slotManager.refresh();
		
		Assert.assertEquals(serverCount - 1, slotManager.allServers().size());;
		AssertBalance(slotManager);
	}

	private void AssertBalance(SlotManager slotManager) {
		
		int average = SlotManager.TOTAL_SLOTS/slotManager.allServers().size();
		for(int serverId : slotManager.allServers()){
			int serverSlotSize = slotManager.getSlotsSizeByServerId(serverId);
			logger.info("[testInit]{}, {}, {}", serverId, serverSlotSize, average);
			Assert.assertTrue((serverSlotSize >= (average -1)) && (serverSlotSize <=  average + 1));
		}			
	}
	
	@Test
	public void simpleTest() throws Exception{
		
		createMetaServers(1);
		sleep(1000);
		TestAppServer server = getLeader();
		server.stop();
	}

	@Test
	public void testShutdownLeader() throws Exception{

		createMetaServers(serverCount);
		sleep(1000);
		TestAppServer leader = null;
		for(TestAppServer server : getServers()){
			if(server.isLeader()){
				logger.info(remarkableMessage("[testShutdownLeader][begin]{}"), server);
				leader = server;
				server.stop();
				logger.info(remarkableMessage("[testShutdownLeader][ end ]{}"), server);
				break;
			}
		}
		
		sleep(TestAppServer.getZksessiontimeoutmillis() + 1000);
		
		TestAppServer newLeader = getLeader();
		logger.info("[testShutdownLeader][new leader]{}", newLeader);
		Assert.assertNotEquals(leader, newLeader);
		
		SlotManager slotManager = newLeader.getContext().getBean(SlotManager.class);
		slotManager.refresh();
		Assert.assertEquals(serverCount - 1, slotManager.allServers().size());;
		AssertBalance(slotManager);
	}

}
