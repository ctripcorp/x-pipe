package com.ctrip.xpipe.redis.console.service;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;

public class ClusterMetaServiceTest extends AbstractConsoleIntegrationTest {
	
	@Autowired
	private ClusterMetaService clusterMetaService;
	@Autowired
	private ClusterService clusterService;
	
	private String clusterName1 = "cluster1";
	private String clusterName2 = "cluster2";
	
	@Test
	@DirtiesContext
	public void testFindNonMigratingClusterMeta() {
		ClusterMeta clusterMetaA = clusterMetaService.getClusterMeta("A", clusterName1);
		ClusterMeta clusterMetaB = clusterMetaService.getClusterMeta("B", clusterName1);
		ClusterTbl cluster = clusterService.find(clusterName1);
		
		Assert.assertEquals(ClusterStatus.Normal.toString(), cluster.getStatus());
		Assert.assertNotNull(clusterMetaA);
		Assert.assertNotNull(clusterMetaB);
		Assert.assertEquals("A", clusterMetaA.getActiveDc());
		Assert.assertEquals("A", clusterMetaB.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testFindMigratingClusterMeta() {
		ClusterTbl cluster = clusterService.find(clusterName2);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), cluster.getStatus());
		
		ClusterMeta clusterMetaA = clusterMetaService.getClusterMeta("A", clusterName2);
		ClusterMeta clusterMetaB = clusterMetaService.getClusterMeta("B", clusterName2);
		Assert.assertEquals("A", clusterMetaA.getActiveDc());
		Assert.assertEquals("B", clusterMetaB.getActiveDc());
	}
}
