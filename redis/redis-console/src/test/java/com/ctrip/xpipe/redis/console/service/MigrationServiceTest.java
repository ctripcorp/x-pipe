package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;

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
	
	private MigrationRequest createEventDemo(long clusterId, long destDcId) {

		MigrationRequest migrationRequest = new MigrationRequest("unit test");
		migrationRequest.setTag("unit test-" + getTestName());

		MigrationRequest.ClusterInfo clusterInfo = new MigrationRequest.ClusterInfo();
		clusterInfo.setClusterId(clusterId);
		clusterInfo.setToDcId(destDcId);
		migrationRequest.addClusterInfo(clusterInfo);
		return migrationRequest;
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
		Assert.assertEquals("unit test", result.getOperator());
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

	@Test(expected = BadRequestException.class)
	public void createMigrationEventWithIncorrectDestDcId() {
		ClusterTbl clusterTbl = clusterService.find(1);
		logger.info("{}", clusterTbl);
		migrationService.createMigrationEvent(createEventDemo(1, -1));
	}

	@Test(expected = BadRequestException.class)
	public void createMigrationEventWithIncorrectDestDcId2() {
		ClusterTbl clusterTbl = clusterService.find(1);
		logger.info("{}", clusterTbl);
		migrationService.createMigrationEvent(createEventDemo(1, 3));
	}
}
