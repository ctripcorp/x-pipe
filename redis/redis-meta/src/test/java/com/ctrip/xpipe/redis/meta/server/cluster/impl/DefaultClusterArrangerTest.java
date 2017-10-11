package com.ctrip.xpipe.redis.meta.server.cluster.impl;


import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


/**
 * @author wenchao.meng
 *
 * Jul 28, 2016
 */
public class DefaultClusterArrangerTest extends AbstractMetaServerContextTest{
	
	@Before
	public void beforeDefaultClusterArrangerTest() throws Exception{
		
		initRegistry();
	}
	
	@Test
	public void testInitArrange() throws Exception{
		
		CuratorFramework client = getCurator();
		client.createContainers(MetaZkConfig.getMetaServerSlotsPath());
		
		List<String> children = client.getChildren().forPath(MetaZkConfig.getMetaServerSlotsPath());
		Assert.assertEquals(0, children.size());
		
		arrangeTaskStart(true);
		startRegistry();
		
		sleep(3000);
		children = client.getChildren().forPath(MetaZkConfig.getMetaServerSlotsPath());
		logger.info("[testInitArrange][getSlots]{}, {}", children.size(), children);
	}
}
