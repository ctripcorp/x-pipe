package com.ctrip.xpipe.redis.console.migration;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;


/**
 * @author shyin
 *
 *         Dec 20, 2016
 */
public class SingleShardMigrationTest extends AbstractMigrationTest {
	private MigrationCluster migrationCluster;
	private MigrationShard migrationShard;

	@Mock
	private MigrationCommandBuilder migrationCommandBuilder;
	@Autowired
	private ClusterMetaService clusterMetaService;
	
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
		for (DcTbl dc : dcService.findClusterRelatedDc("cluster1")) {
			dcs.put(dc.getId(), dc);
		}
		migrationShard = new DefaultMigrationShard(migrationCluster, migrationService.findMigrationShards(1).get(0),
				shardService.find(1), dcs, migrationService, migrationCommandBuilder);
		migrationCluster.addNewMigrationShard(migrationShard);
		
	}
	
	@Test
	@DirtiesContext
	public void testSuccess() {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B");
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A");
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B");
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A");
	
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("B", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("B", newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testCheckFail() {
		mockFailCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B");
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A");
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B");
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A");
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.CHECK));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		Assert.assertFalse(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.CHECK).equals(""));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("A", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("A", newPrimaryDcMeta.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testCheckExceptionFail() {
		mockFailCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B", new Throwable("mocked check fail"));
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A");
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B");
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A");
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.CHECK));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		Assert.assertFalse(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.CHECK).equals(""));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("A", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("A", newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testMigratePrevExceptionFail() {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B");
		mockFailPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A", new Throwable("mocked prev fail"));
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B");
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A");
	
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		String message = migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC).getRight();
		Assert.assertTrue(message.indexOf("Ignore:java.lang.Throwable: mocked prev fail") > 0);
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("B", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("B", newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testMigrateNewFail() {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B");
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A");
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B");
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A");
	
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), currentCluster.getStatus());
		Assert.assertEquals(MigrationStatus.PartialSuccess, migrationCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("A", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("B", newPrimaryDcMeta.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testMigrateNewExceptionFail() {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B");
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A");
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B",new Throwable("mocked new fail"));
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A");
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(MigrationStatus.PartialSuccess, migrationCluster.getStatus());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("A", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("B", newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testMigrateOtherDcFail() {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B");
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A");
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B");
		mockFailOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A");
	
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("B", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("B", newPrimaryDcMeta.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testMigrateOtherDcExceptionFail() {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "B");
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "A");
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B");
		mockFailOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", "B", "A", new Throwable("mocked other fail"));
	
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals(1, originalCluster.getActivedcId());
		Assert.assertEquals(1, migrationCluster.getMigrationCluster().getSourceDcId());
		Assert.assertEquals(2, migrationCluster.getMigrationCluster().getDestinationDcId());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		migrationCluster.process();
		sleep(1000);
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta("A", "cluster1");
		Assert.assertEquals("B", prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta("B", "cluster1");
		Assert.assertEquals("B", newPrimaryDcMeta.getActiveDc());
	}

}
