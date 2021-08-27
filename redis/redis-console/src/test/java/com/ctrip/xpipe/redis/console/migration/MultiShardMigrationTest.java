package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.migration.AbstractOuterClientService;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.model.*;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
/**
 * @author shyin
 *
 * Dec 20, 2016
 */
public class MultiShardMigrationTest extends AbstractMigrationTest {

	private static int TEST_SHARD_CNT = 32;
	private MigrationCluster migrationCluster;
	
	@Mock
	private MigrationCommandBuilder migrationCommandBuilder;

	@Mock
	private MigrationEvent migrationEvent;
	
	private String clusterName = "cluster1";

	private long clusterId = 1;

	private String dcA = dcNames[0];
	private String dcB = dcNames[1];

	@Override
	public String prepareDatas() {
		try {
			return prepareDatasFromFile("src/test/resources/single-shard-migration-test.sql");
		} catch (Exception ex) {
			logger.error("Prepare data from file failed", ex);
		}
		return "";
	}
	
	@Before
	public void prepare() {

		MockitoAnnotations.initMocks(this);

		Map<Long, DcTbl> dcs = new HashMap<>();
		for(DcTbl dc : dcService.findClusterRelatedDc(clusterName)) {
			dcs.put(dc.getId(), dc);
		}

		createShards();

		MigrationClusterTbl migrationClusterTbl = migrationService.findMigrationCluster(1L, clusterId);
		migrationCluster = new DefaultMigrationCluster(executors, scheduled, migrationEvent, migrationClusterTbl, dcService, clusterService, shardService, redisService, migrationService);

		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++cnt) {

			MigrationShardTbl migrationShardTbl = new MigrationShardTbl();
			migrationShardTbl.setId(cnt).setMigrationClusterId(1).setShardId(cnt).setLog("");

			ShardTbl shardTbl = new ShardTbl();
			shardTbl.setId(cnt).setClusterId(clusterId).setShardName(getShardName(cnt)).setSetinelMonitorName("cluster1-" + getShardName(cnt));
			MigrationShard migrationShard = new DefaultMigrationShard(migrationCluster, migrationShardTbl, shardTbl, dcs, migrationService, migrationCommandBuilder);
			migrationCluster.addNewMigrationShard(migrationShard);
		}
	}

	@Test
	@DirtiesContext
	public void testAllSuccess() throws TimeoutException {

		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++ cnt) {
			mockSuccessCheckCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcB);
			mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcA);
			mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB);
			mockSuccessOtherDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcA);
			
		}
		ClusterTbl originalCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals(MigrationStatus.Initiated.toString(), migrationCluster.getStatus().toString());
		Assert.assertEquals(TEST_SHARD_CNT, migrationCluster.getMigrationShards().size());
		for(MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
			Assert.assertEquals(ShardMigrationResultStatus.FAIL,migrationShard.getShardMigrationResult().getStatus());
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		}
		
		migrationCluster.process();

		AtomicReference<ClusterTbl> currentCluster = new AtomicReference<>();

		waitConditionUntilTimeOut(() -> {
			currentCluster.set(clusterService.find(clusterId));
			return ClusterStatus.Normal.toString().equalsIgnoreCase(currentCluster.get().getStatus());

		});

		Assert.assertEquals(2, currentCluster.get().getActivedcId());
		Assert.assertEquals(MigrationStatus.Success.toString(), migrationCluster.getStatus().toString());

		for(MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
			Assert.assertEquals(ShardMigrationResultStatus.SUCCESS,migrationShard.getShardMigrationResult().getStatus());
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		}
	}

	@Test
	@DirtiesContext
	public void testCRedisCheckFailed() throws TimeoutException {

		int failPos = randomInt(1, TEST_SHARD_CNT);
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++ cnt) {
			mockSuccessCheckCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcB);
			mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcA);
			mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB);
			mockSuccessOtherDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcA);
		}
		((DefaultMigrationCluster)migrationCluster).setOuterClientService(new AbstractOuterClientService() {
			@Override
			public ClusterInfo getClusterInfo(String clusterName) throws Exception {
				return null;
			}

			@Override
			public DcMeta getOutClientDcMeta(String dc) throws Exception {
				return null;
			}
		});

		ClusterTbl originalCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());

		migrationCluster.process();
		sleep(1000);
		Assert.assertEquals(MigrationStatus.CheckingFail, migrationCluster.getStatus());
		ClusterTbl currentCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Lock.toString(), currentCluster.getStatus());


		((DefaultMigrationCluster)migrationCluster).setOuterClientService(new AbstractOuterClientService() {

			@Override
			public DcMeta getOutClientDcMeta(String dc) throws Exception {
				return null;
			}

			@Override
			public ClusterInfo getClusterInfo(String clusterName) throws Exception {
				ClusterInfo clusterInfo = new ClusterInfo();
				clusterInfo.setGroups(Lists.newArrayList(new GroupInfo()));
				return clusterInfo;
			}

			@Override
			public boolean clusterMigratePreCheck(String clusterName) throws OuterClientException {
				return true;
			}
		});
		//again
		migrationCluster.process();
		waitConditionUntilTimeOut(()-> clusterService.find(clusterId).getStatus().equals(ClusterStatus.Normal.toString()), 2500);
		currentCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());

	}


	@Test
	@DirtiesContext
	public void testOneFailedOnChecking() {

		int failPos = randomInt(1, TEST_SHARD_CNT);
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++ cnt) {
			if (cnt == failPos) {
				mockFailCheckCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcB, new Throwable("mocked check fail"));
			} else {
				mockSuccessCheckCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcB);
			}
			mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcA);
			mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB);
			mockSuccessOtherDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcA);
		}

		ClusterTbl originalCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals(MigrationStatus.Initiated.toString(), migrationCluster.getStatus().toString());
		Assert.assertEquals(TEST_SHARD_CNT, migrationCluster.getMigrationShards().size());		
		for(MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
			Assert.assertEquals(ShardMigrationResultStatus.FAIL,migrationShard.getShardMigrationResult().getStatus());
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		}
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Lock.toString(), currentCluster.getStatus());
		Assert.assertEquals(MigrationStatus.CheckingFail, migrationCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		for(MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
			if(migrationShard.getCurrentShard().getId() == failPos) {
				Assert.assertEquals(ShardMigrationResultStatus.FAIL,migrationShard.getShardMigrationResult().getStatus());
				Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
				Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
				Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
				Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
				Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
				continue;
			}
			
			Assert.assertEquals(ShardMigrationResultStatus.FAIL,migrationShard.getShardMigrationResult().getStatus());
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		}

		//
		logger.info("[testOneFailedOnChecking][retry success]");
		mockSuccessCheckCommand(migrationCommandBuilder,clusterName, getShardName(failPos), dcB, dcB);
		migrationCluster.process();
		sleep(1500);
		currentCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
	}
	
	@Test
	@DirtiesContext
	public void testOneFailedOnMigration() {

		int failPos = randomInt(1, TEST_SHARD_CNT);
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++ cnt) {
			mockSuccessCheckCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcB);
			mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcA);
			if(cnt == failPos) {
				mockFailNewPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, new Throwable("mocked new fail"));
			} else {
				mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB);
			}
			mockSuccessOtherDcCommand(migrationCommandBuilder,clusterName, getShardName(cnt), dcB, dcA);
		}
		ClusterTbl originalCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals(MigrationStatus.Initiated.toString(), migrationCluster.getStatus().toString());
		Assert.assertEquals(TEST_SHARD_CNT, migrationCluster.getMigrationShards().size());		
		for(MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
			Assert.assertEquals(ShardMigrationResultStatus.FAIL,migrationShard.getShardMigrationResult().getStatus());
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		}
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(MigrationStatus.PartialRetryFail.toString(), migrationCluster.getStatus().toString());
		for(MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
			if(migrationShard.getCurrentShard().getId() == failPos) {
				Assert.assertEquals(ShardMigrationResultStatus.FAIL,migrationShard.getShardMigrationResult().getStatus());
				Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
				Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
				Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
				Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_OTHER_DC));
				Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
				continue;
			}
			
			Assert.assertEquals(ShardMigrationResultStatus.FAIL,migrationShard.getShardMigrationResult().getStatus());
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
			Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
			Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_OTHER_DC));
			Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		}
		Assert.assertEquals(MigrationStatus.PartialRetryFail, migrationCluster.getStatus());


		logger.info("[testOneFailedOnMigration][retry success]");
		//retry success
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,clusterName, getShardName(failPos), dcB);
		migrationCluster.process();
		sleep(1500);
		currentCluster = clusterService.find(clusterId);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());


	}
	
	private String getShardName(int id) {
		return "shard" + Integer.toString(id);
	}

	private void createShards() {
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++cnt) {
			ShardTbl shardTbl = new ShardTbl();
			shardTbl.setId(cnt).setClusterId(clusterId).setShardName(getShardName(cnt)).setSetinelMonitorName("cluster1-" + getShardName(cnt));
			if(cnt != 1) {
				shardService.createShard(clusterName,
						(new ShardTbl()).setShardName(getShardName(cnt)).setClusterId(clusterId)
								.setSetinelMonitorName(clusterName + "-" + getShardName(cnt)),new HashMap<>());
			}
		}
	}

}
