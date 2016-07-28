package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;


/**
 * @author wenchao.meng
 *
 * Jul 28, 2016
 */
public class DefaultClusterArrangerTest extends AbstractMetaServerTest{
	
	@Before
	public void beforeDefaultClusterArrangerTest() throws Exception{
		
		initRegistry();
	}
	
	@Test
	public void testInitArrange() throws Exception{
		
		CuratorFramework client = getCurator();
		EnsurePath ensure = client.newNamespaceAwareEnsurePath(MetaZkConfig.getMetaServerSlotsPath());
		ensure.ensure(client.getZookeeperClient());
		
		List<String> children = client.getChildren().forPath(MetaZkConfig.getMetaServerSlotsPath());
		Assert.assertEquals(0, children.size());
		
		arrangeTaskStart(true);
		startRegistry();
		
		sleep(2000);
		
		logger.info("[testInitArrange][getSlots]");
		children = client.getChildren().forPath(MetaZkConfig.getMetaServerSlotsPath());
		logger.info("[testInitArrange][getSlots]{}", children);
		Assert.assertEquals(SlotManager.TOTAL_SLOTS, children.size());
	}
	
	@Test
	public void testAddServerArrange() throws Exception{
		
		int newServerID = 100;
		
		startRegistry();
		SlotManager slotManager = getBean(SlotManager.class);
		sleep(5000);
		slotManager.refresh();
		
		DefaultMetaServerConfig metaServerConfig = new DefaultMetaServerConfig();
		metaServerConfig.setDefaultMetaServerId(newServerID);
		createAndStart(metaServerConfig);
		
		sleep(2000);
		
	}


}
