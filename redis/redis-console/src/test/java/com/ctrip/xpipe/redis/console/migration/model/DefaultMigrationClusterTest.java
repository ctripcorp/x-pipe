package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.migration.AbstractMigrationTest;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationSuccessState;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *
 *         Dec 29, 2016
 */
public class DefaultMigrationClusterTest extends AbstractMigrationTest {
	private MigrationCluster migrationCluster;
	private MigrationShard migrationShard;

	@Mock
	private MigrationCommandBuilder migrationCommandBuilder;
	@Mock
	private MigrationEvent migrationEvent;
	@Autowired
	private MigrationService migrationService;
	@Autowired
	private DcMetaService dcMetaService;

	private String dcA;
	private String dcB;
	
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

		dcA = dcNames[0];
		dcB = dcNames[1];

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
	public void testCancelOnInitiated() {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
		
		ClusterTbl originalCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Lock.toString(), originalCluster.getStatus());
		Assert.assertEquals("Initiated", migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));

		migrationCluster.cancel();
		
		ClusterTbl afterCacelledCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), afterCacelledCluster.getStatus());
		Assert.assertEquals(MigrationStatus.Aborted.toString(), migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		
		MigrationClusterTbl migrationCluster = migrationService.findMigrationCluster(1, 1);
		Assert.assertEquals(MigrationStatus.Aborted.toString(), migrationCluster.getStatus());
	}
	
	@Test
	@DirtiesContext
	public void testCancelOnChecking() {
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
		Assert.assertEquals(ClusterStatus.Lock.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
		Assert.assertFalse(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.CHECK).equals(""));

		migrationCluster.cancel();
		ClusterTbl afterCacelledCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), afterCacelledCluster.getStatus());
		Assert.assertEquals(MigrationStatus.Aborted.toString(), migrationCluster.getStatus().toString());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		
		MigrationClusterTbl migrationCluster = migrationService.findMigrationCluster(1, 1);
		Assert.assertEquals(MigrationStatus.Aborted.toString(), migrationCluster.getStatus());
	}
	
	@Test
	@DirtiesContext
	public void testCancelOnMigrating() throws Exception {
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
		DcMeta DcAMeta = dcMetaService.getDcMeta(dcA);
		DcMeta DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcB, DcBMeta.findCluster("cluster1").getActiveDc());
		
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Migrating.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
		Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
		Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertNull(migrationShard.getShardMigrationResult().getSteps().get(ShardMigrationStep.MIGRATE_OTHER_DC));
		Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));

		migrationCluster.cancel();
		
		DcAMeta = dcMetaService.getDcMeta(dcA);
		DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcA, DcBMeta.findCluster("cluster1").getActiveDc());
	}
	
	@Test
	@DirtiesContext
	public void testRollBackOnPartialSuccess() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB,new Throwable("mocked new fail"));
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
		mockSuccessRollBackCommand(migrationCommandBuilder, "cluster1", "shard1", dcA);

		migrationCluster.process();
		sleep(1000);
		DcMeta DcAMeta = dcMetaService.getDcMeta(dcA);
		DcMeta DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcB, DcBMeta.findCluster("cluster1").getActiveDc());
		
		Assert.assertEquals(MigrationStatus.PartialRetryFail, migrationCluster.getStatus());

		migrationCluster.rollback();
		sleep(1000);
		DcAMeta = dcMetaService.getDcMeta(dcA);
		DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcA, DcBMeta.findCluster("cluster1").getActiveDc());
		
		Assert.assertEquals(MigrationStatus.Aborted, migrationCluster.getStatus());
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
	}
	
	@Test
	@DirtiesContext
	public void testRollBackFailOnPartialSuccess() throws Exception {

		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB,new Throwable("mocked new fail"));
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
		mockFailRollBackCommand(migrationCommandBuilder, "cluster1", "shard1", dcA);

		migrationCluster.process();
		sleep(1000);
		DcMeta DcAMeta = dcMetaService.getDcMeta(dcA);
		DcMeta DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcB, DcBMeta.findCluster("cluster1").getActiveDc());
		
		Assert.assertEquals(MigrationStatus.PartialRetryFail, migrationCluster.getStatus());

		migrationCluster.rollback();
		sleep(1000);
		DcAMeta = dcMetaService.getDcMeta(dcA);
		DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcA, DcBMeta.findCluster("cluster1").getActiveDc());
		
		Assert.assertEquals(MigrationStatus.RollBackFail, migrationCluster.getStatus());
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals(ClusterStatus.Rollback.toString(), currentCluster.getStatus());
		Assert.assertEquals(1, currentCluster.getActivedcId());
	}
	
	@Test
	@DirtiesContext
	public void testRollBackOnChecking() throws Exception {
		mockFailCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockSuccessNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB);
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);

		migrationCluster.process();
		sleep(1000);
		
		DcMeta DcAMeta = dcMetaService.getDcMeta(dcA);
		DcMeta DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcA, DcBMeta.findCluster("cluster1").getActiveDc());
		
		Assert.assertEquals(MigrationStatus.CheckingFail, migrationCluster.getStatus());

		migrationCluster.rollback();
	}
	
	@Test
	@DirtiesContext
	public void testForcePublishOnPartialSuccess() throws Exception {
		mockSuccessCheckCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcB);
		mockSuccessPrevPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcA);
		mockFailNewPrimaryDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB,new Throwable("mocked new fail"));
		mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);

		migrationCluster.process();
		sleep(1000);
		DcMeta DcAMeta = dcMetaService.getDcMeta(dcA);
		DcMeta DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcA, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcB, DcBMeta.findCluster("cluster1").getActiveDc());
		
		Assert.assertEquals(MigrationStatus.PartialRetryFail, migrationCluster.getStatus());

		migrationCluster.forceProcess();
		DcAMeta = dcMetaService.getDcMeta(dcA);
		DcBMeta = dcMetaService.getDcMeta(dcB);
		Assert.assertEquals(dcB, DcAMeta.findCluster("cluster1").getActiveDc());
		Assert.assertEquals(dcB, DcBMeta.findCluster("cluster1").getActiveDc());
		
		Assert.assertEquals(MigrationStatus.Success, migrationCluster.getStatus());
		ClusterTbl currentCluster = clusterService.find(1);
		Assert.assertEquals("Normal", currentCluster.getStatus());
		Assert.assertEquals(2, currentCluster.getActivedcId());
	}

	@Test
	@DirtiesContext
	public void testConcurrentUpdateStat() throws Exception {
		CountDownLatch latch = new CountDownLatch(3);
		executors.execute(new Runnable() {
			@Override
			public void run() {
				try {
					migrationCluster.updateStat(new MigrationCheckingState(migrationCluster));
				} catch (Exception e) {
					logger.info("[testConcurrentUpdateStat] update checking fail", e);
				}
				latch.countDown();
			}
		});
		executors.execute(new Runnable() {
			@Override
			public void run() {
				try {
					migrationCluster.updateStat(new MigrationPublishState(migrationCluster));
				} catch (Exception e) {
					logger.info("[testConcurrentUpdateStat] update checking fail", e);
				}
				latch.countDown();
			}
		});
		executors.execute(new Runnable() {
			@Override
			public void run() {
				try {
					migrationCluster.updateStat(new MigrationSuccessState(migrationCluster));
				} catch (Exception e) {
					logger.info("[testConcurrentUpdateStat] update checking fail", e);
				}
				latch.countDown();
			}
		});

		latch.await(5000, TimeUnit.MILLISECONDS);
		MigrationClusterTbl migrationClusterTbl = migrationService.findMigrationCluster(1L, 1L);
		ClusterTbl clusterTbl = clusterService.find("cluster1");
		MigrationStatus migrationStatus = MigrationStatus.valueOf(migrationClusterTbl.getStatus());
		logger.info("[testConcurrentUpdateStat] migration status {}", migrationClusterTbl.getStatus());
		logger.info("[testConcurrentUpdateStat] cluster status {}", clusterTbl.getStatus());
		Assert.assertEquals(clusterTbl.getStatus(), migrationStatus.getClusterStatus().name());
	}
	
}
