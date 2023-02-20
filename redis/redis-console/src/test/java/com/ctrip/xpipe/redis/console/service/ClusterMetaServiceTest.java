package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.Assert.fail;

public class ClusterMetaServiceTest extends AbstractConsoleIntegrationTest {
	
	@Autowired
	private DcMetaService dcMetaService;
	@Autowired
	private ClusterMetaService clusterMetaService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	
	private String clusterName1 = "cluster1";
	private String clusterName2 = "cluster2";

	private String dcA = dcNames[0];
	private String dcB = dcNames[1];

	@Override
	public String prepareDatas() {
		try {
			return prepareDatasFromFile("src/test/resources/migration-test.sql");
		} catch (Exception ex) {
			logger.error("Prepare data from file failed",ex);
		}
		return "";
	}
	
	@Test
	@DirtiesContext
	public void testFindNonMigratingClusterMeta() {
		ClusterMeta clusterMetaA = clusterMetaService.getClusterMeta(dcA, clusterName1);
		ClusterMeta clusterMetaB = clusterMetaService.getClusterMeta(dcB, clusterName1);
		ClusterTbl cluster = clusterService.find(clusterName1);
		
		Assert.assertEquals(ClusterStatus.Normal.toString(), cluster.getStatus());
		Assert.assertNotNull(clusterMetaA);
		Assert.assertNotNull(clusterMetaB);
		Assert.assertEquals(dcA, clusterMetaA.getActiveDc());
		Assert.assertEquals(dcA, clusterMetaB.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testFindMigratingClusterMeta() {
		ClusterTbl cluster = clusterService.find(clusterName2);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), cluster.getStatus());
		
		ClusterMeta clusterMetaA = clusterMetaService.getClusterMeta(dcA, clusterName2);
		ClusterMeta clusterMetaB = clusterMetaService.getClusterMeta(dcB, clusterName2);
		Assert.assertEquals(dcA, clusterMetaA.getActiveDc());
		Assert.assertEquals(dcB, clusterMetaB.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testGetDifferentActiveDcForDcMetaWhileMigrating() throws Exception {
		ClusterTbl clusterA = clusterService.find(clusterName1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), clusterA.getStatus());
		ClusterTbl clusterB = clusterService.find(clusterName2);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), clusterB.getStatus());
		
		DcMeta dcAMeta = dcMetaService.getDcMeta(dcA);
		DcMeta dcBMeta = dcMetaService.getDcMeta(dcB);
		
		Assert.assertEquals(dcA, dcAMeta.findCluster(clusterName1).getActiveDc());
		Assert.assertEquals(dcA, dcBMeta.findCluster(clusterName1).getActiveDc());
		Assert.assertEquals(dcA, dcAMeta.findCluster(clusterName2).getActiveDc());
		Assert.assertEquals(dcB, dcBMeta.findCluster(clusterName2).getActiveDc());

	}

    @Test
    public void testGetNullShardClusterMeta() throws Exception {
        shardService.deleteShard(clusterName1, "shard1");
        shardService.deleteShard(clusterName1, "shard2");

        try {
            ClusterMeta clusterMeta = clusterMetaService.getClusterMeta(dcA, clusterName1);
            ObjectMapper objectMapper = new ObjectMapper();
            logger.info("{}", objectMapper.writeValueAsString(clusterMeta));
        } catch (Throwable t) {
            fail();
        }
    }

    @Test
	public void testGetClusterMeta() throws Exception {
		ClusterMeta clusterMeta = clusterMetaService.getClusterMeta(dcA, clusterName1);
		Assert.assertEquals(1L, clusterMeta.getDbId().longValue());
		for (ShardMeta shardMeta: clusterMeta.getShards().values()) {
			Assert.assertNotNull(shardMeta.getDbId());
		}
	}
}
