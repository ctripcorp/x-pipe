package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

public class MigrationServiceTest extends AbstractConsoleIntegrationTest {
	
	@Autowired
	private MigrationService migrationService;
	@Autowired
	private ClusterService clusterService;

	@Override
	public String prepareDatas() {
		try {
			return prepareDatasFromFile("src/test/resources/migration-test.sql");
		} catch (Exception ex) {
			logger.error("Prepare data from file failed",ex);
		}
		return "";
	}
	
	private MigrationEventModel createEventDemo(long clusterId, long destDcId) {
		MigrationEventModel model = new MigrationEventModel();
		MigrationEventTbl event = new MigrationEventTbl();
		MigrationClusterTbl migrationClsuter = new MigrationClusterTbl();
		migrationClsuter.setClusterId(clusterId).setDestinationDcId(destDcId);
		
		event.getMigrationClusters().add(migrationClsuter);
		model.setEvent(event);
		return model;
	}
	
	@Test
	@DirtiesContext
	public void createTest() throws ComponentLookupException {
		long eventId = migrationService.createMigrationEvent(createEventDemo(1,2));
		MigrationEventTbl result = migrationService.find(eventId);
		MigrationClusterTbl result_cluster = migrationService.findMigrationCluster(eventId, 1);
		ClusterTbl cluster = clusterService.find(1);
		List<MigrationShardTbl> result_shards = migrationService.findMigrationShards(result_cluster.getId());
		
		Assert.assertEquals(eventId, result.getId());
		Assert.assertEquals("xpipe", result.getOperator());
		Assert.assertNotNull(result_cluster);
		Assert.assertEquals(1, result_cluster.getClusterId());
		Assert.assertEquals(2, result_cluster.getDestinationDcId());
		Assert.assertEquals(MigrationStatus.Initiated.toString(), result_cluster.getStatus());
		Assert.assertEquals(ClusterStatus.Lock.toString(), cluster.getStatus());
		Assert.assertEquals(2, result_shards.size());
	}
	
	@Test
	@DirtiesContext
	public void clusterConflictTest() throws ComponentLookupException {
		migrationService.createMigrationEvent(createEventDemo(1,2));
		try {
			migrationService.createMigrationEvent(createEventDemo(1,2));
		} catch (Exception ex) {
			Assert.assertEquals("Cluster:cluster1 already under migrating tasks!Please verify it first!", ex.getMessage());
		}
	}

}
