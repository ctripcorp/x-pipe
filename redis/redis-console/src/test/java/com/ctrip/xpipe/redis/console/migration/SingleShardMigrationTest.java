package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.model.*;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;


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

	@Mock
	private MigrationEvent migrationEvent;

	@Autowired
	private ClusterMetaService clusterMetaService;

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
		
		MigrationClusterTbl migrationClusterTbl = migrationService.findMigrationCluster(1L, 1L);
		migrationCluster = new DefaultMigrationCluster(executors, scheduled, migrationEvent, migrationClusterTbl, dcService, clusterService, shardService, redisService, migrationService);
		
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
	public void testBugConcurrentModifyMigrationState() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockFailThenSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, new TimeoutException("metaserver 500"));

		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);


		ClusterTbl originalCluster = clusterService.find(1);

		migrationCluster.process();
		sleep(2000);

		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));

		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));

		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcB, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());

	}


	@Test
	@DirtiesContext
	public void testSuccess() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
	
		
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
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcB, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testCheckFail() throws Exception {
		mockFailCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
		
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
		Assert.assertEquals(MigrationStatus.CheckingFail, migrationCluster.getStatus());;

		Assert.assertEquals(ClusterStatus.Lock.toString(), currentCluster.getStatus());
		Assert.assertEquals(migrationCluster.getMigrationCluster().getMigrationEventId(), currentCluster.getMigrationEventId());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		Assert.assertFalse(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.CHECK).equals(""));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcA, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcA, newPrimaryDcMeta.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testCheckExceptionFail() throws Exception {
		mockFailCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB, new Throwable("mocked check fail"));
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
		
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
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcA, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcA, newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testMigratePrevExceptionFail() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockFailPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA, new Throwable("mocked prev fail"));
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
	
		
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
		String message = migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC).getValue();
		Assert.assertTrue(message.indexOf("Ignore:mocked prev fail") > 0);
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcB, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testMigrateNewFail() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
	
		
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
		Assert.assertEquals(MigrationStatus.PartialRetryFail, migrationCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcA, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testMigrateNewExceptionFail() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB,new Throwable("mocked new fail"));
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
		
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
		Assert.assertEquals(MigrationStatus.PartialRetryFail, migrationCluster.getStatus());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcA, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());
	}

	@Test
	@DirtiesContext
	public void testMigrateNewFailAndRetrySuccess() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB,new Throwable("mocked new fail"));
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);

		migrationCluster.process();
		waitConditionUntilTimeOut(() -> migrationCluster.getStatus().equals(MigrationStatus.PartialRetryFail), 3000, 200);

		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(MigrationStatus.PartialRetryFail, migrationCluster.getStatus());

		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder, "cluster1", "shard1", dcB);

		migrationCluster.process();
		waitConditionUntilTimeOut(() -> ClusterStatus.Normal.toString().equals(clusterService.find(1).getStatus()), 3000, 200);
		currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, migrationShard.getShardMigrationResult().getStatus());
	}

	@Test
	@DirtiesContext
	public void testMigrateOtherDcFail() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockFailOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
	
		
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
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcB, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testMigrateOtherDcExceptionFail() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockFailOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA, new Throwable("mocked other fail"));
	
		
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
		
		ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
		Assert.assertEquals(dcB, prevPrimaryDcMeta.getActiveDc());
		ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
		Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());
	}

}
