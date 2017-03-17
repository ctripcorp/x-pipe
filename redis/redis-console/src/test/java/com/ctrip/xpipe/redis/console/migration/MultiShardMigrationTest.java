package com.ctrip.xpipe.redis.console.migration;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.annotation.DirtiesContext;

import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationResultStatus;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
/**
 * @author shyin
 *
 * Dec 20, 2016
 */
public class MultiShardMigrationTest extends AbstractMigrationTest {
	private static int TEST_SHARD_CNT = 10;
	
	private MigrationCluster migrationCluster;
	
	@Mock
	private MigrationCommandBuilder migrationCommandBuilder;
	
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
		
		MigrationClusterTbl migrationClusterTbl = migrationService.findMigrationCluster(1L, 1L);
		migrationCluster = new DefaultMigrationCluster(migrationClusterTbl, dcService, clusterService, shardService, redisService, migrationService);
		
		Map<Long, DcTbl> dcs = new HashMap<>();
		for(DcTbl dc : dcService.findClusterRelatedDc("cluster1")) {
			dcs.put(dc.getId(), dc);
		}
		
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++cnt) {
			MigrationShardTbl migrationShardTbl = new MigrationShardTbl();
			migrationShardTbl.setId(cnt).setMigrationClusterId(1).setShardId(cnt).setLog("");
			
			ShardTbl shardTbl = new ShardTbl();
			shardTbl.setId(cnt).setClusterId(1).setShardName(getShardName(cnt)).setSetinelMonitorName("cluster1-" + getShardName(cnt));

			MigrationShard migrationShard = new DefaultMigrationShard(migrationCluster, migrationShardTbl, shardTbl, dcs, migrationService, migrationCommandBuilder);
			migrationCluster.addNewMigrationShard(migrationShard);
		}
	}
	
	@Test
	@DirtiesContext
	public void testAllSuccess() {
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++ cnt) {
			mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", "B");
			mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "A");
			mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B");
			mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", "A");
			
			if(cnt != 1) {
				migrationCluster.getShardService().createShard("cluster1", (new ShardTbl()).setShardName(getShardName(cnt)).setClusterId(1)
						.setSetinelMonitorName("cluster1" + "-" + getShardName(cnt)),new HashMap<Long, SetinelTbl>());
			}
		}
		ClusterTbl originalCluster = clusterService.find(1);
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
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
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
	public void testOneFailedOnChecking() {
		int failPos = randomInt(1, TEST_SHARD_CNT);
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++ cnt) {
			if (cnt == failPos) {
				mockFailCheckCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", "B", new Throwable("mocked check fail"));
			} else {
				mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", "B");
			}
			mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "A");
			mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B");
			mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", "A");
		}
		ClusterTbl originalCluster = clusterService.find(1);
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
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(MigrationStatus.Checking.toString(), migrationCluster.getStatus().toString());
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
	}
	
	@Test
	@DirtiesContext
	public void testOneFailedOnMigration() {
		int failPos = randomInt(1, TEST_SHARD_CNT);
		for(int cnt = 1 ; cnt != TEST_SHARD_CNT + 1; ++ cnt) {
			mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", "B");
			mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "A");
			if(cnt == failPos) {
				mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", new Throwable("mocked new fail"));
			} else {
				mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B");
			}
			mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", getShardName(cnt), "B", "A");
		}
		ClusterTbl originalCluster = clusterService.find(1);
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
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(MigrationStatus.PartialSuccess.toString(), migrationCluster.getStatus().toString());
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
		Assert.assertEquals(MigrationStatus.PartialSuccess, migrationCluster.getStatus());
	}
	
	private String getShardName(int id) {
		return "shard" + Integer.toString(id);
	}
}
