package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationLock;
import com.ctrip.xpipe.redis.console.migration.status.migration.statemachine.Doing;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblEntity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.unidal.lookup.ContainerLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * @author lishanglin
 * date 1010/12/18
 */
public class MigrationExecuteLockTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationServiceImpl migrationService;

    @Autowired
    private MigrationEventDao migrationEventDao;

    private MigrationEventManager migrationEventManager;

    private DefaultMigrationEvent event;

    private DefaultMigrationEvent otherEvent;

    private Map<Long, MigrationCluster> originClusters;

    private Map<Long, MigrationCluster> originClustersForOtherDc;

    private MigrationEventTblDao migrationEventTblDao;

    @Before
    public void setupMigrationExecuteLockTest() throws Exception {
        migrationEventManager = migrationService.getMigrationEventManager();
        event = (DefaultMigrationEvent) migrationEventManager.getEvent(1);
        otherEvent = (DefaultMigrationEvent) migrationEventDao.buildMigrationEvent(1);

        originClusters = hackMigrationClusters(event);
        originClustersForOtherDc = hackMigrationClusters(otherEvent);

        event.setMigrationLock(new DefaultMigrationLock(1, 1000, migrationEventDao, "jq"));
        otherEvent.setMigrationLock(new DefaultMigrationLock(1, 1000, migrationEventDao, "oy"));

        migrationEventTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationEventTblDao.class);
    }

    @After
    public void afterMigrationExecuteLockTest() {
        restoreMigrationClusters();
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessInitiated() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(2,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackInitiated() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelInitiated() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishInitiated() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndInitiated() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessChecking() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(3,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackChecking() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelChecking() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishChecking() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndChecking() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessCheckingFail() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(4,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackCheckingFail() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelCheckingFail() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishCheckingFail() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndCheckingFail() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessMigrating() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(5,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackMigrating() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelMigrating() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishMigrating() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndMigrating() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessPublish() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(6,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackPublish() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelPublish() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishPublish() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndPublish() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessPartialSuccess() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(1,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackPartialSuccess() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelPartialSuccess() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishPartialSuccess() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndPartialSuccess() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessRollBack() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(7,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackRollBack() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelRollBack() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishRollBack() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndRollBack() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessAborted() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(8,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackAborted() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelAborted() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishAborted() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndAborted() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessSuccess() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(9,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackSuccess() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelSuccess() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishSuccess() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndSuccess() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessForceEnd() throws Exception {
        testConcurrentProcessExecuteOneTimeAndLockRelease(10,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackForceEnd() throws Exception {
        testConcurrentRollbackExecuteOneTimeAndLockRelease(10, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelForceEnd() throws Exception {
        testConcurrentCancelExecuteOneTimeAndLockRelease(10, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishForceEnd() throws Exception {
        testConcurrentForcePublishExecuteOneTimeAndLockRelease(10, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndForceEnd() throws Exception {
        testConcurrentForceEndExecuteOneTimeAndLockRelease(10, 10);
    }

    @Test
    @DirtiesContext
    public void concurrentProcessAndRollBack() throws Exception {
        int concurrent = 100;
        CyclicBarrier barrier = new CyclicBarrier(concurrent);
        CountDownLatch countDownLatch = new CountDownLatch(concurrent);

        IntStream.range(0, concurrent).forEach(i -> {
            executors.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    logger.info("[testConcurrentProcess] barrier fail", e);
                }

                try {
                    if (i % 2 > 0) {
                        migrationService.rollbackMigrationCluster(1, 1);
                    } else {
                        migrationService.continueMigrationCluster(1, 2);
                    }
                } catch (Exception e) {

                }
                countDownLatch.countDown();
            });
        });

        countDownLatch.await(5000, TimeUnit.MILLISECONDS);
        waitConditionUntilTimeOut(() -> !event.isRunning());

        Assert.assertEquals("", getExecLock());
    }
    
    // Test migration from multi dc
    
    @Test
    @DirtiesContext
    public void testConcurrentProcessInitiatedFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollbackInitiatedFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelInitiatedFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishInitiatedFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndInitiatedFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(2, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessCheckingFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollbackCheckingFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelCheckingFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishCheckingFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndCheckingFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(3, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessCheckingFailFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(4,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackCheckingFailFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelCheckingFailFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishCheckingFailFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndCheckingFailFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(4, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessMigratingFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(5,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackMigratingFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelMigratingFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishMigratingFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndMigratingFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(5, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessPublishFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(6,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackPublishFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelPublishFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishPublishFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndPublishFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(6, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessPartialSuccessFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(1,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackPartialSuccessFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelPartialSuccessFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishPartialSuccessFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndPartialSuccessFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(1, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessRollBackFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(7,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackRollBackFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelRollBackFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishRollBackFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndRollBackFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(7, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessAbortedFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(8,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackAbortedFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelAbortedFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishAbortedFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndAbortedFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(8, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessSuccessFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(9,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackSuccessFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelSuccessFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishSuccessFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndSuccessFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(9, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentProcessForceEndFromMultiDc() throws Exception {
        testConcurrentProcessLockReleaseFromMultiDc(10,10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentRollBackForceEndFromMultiDc() throws Exception {
        testConcurrentRollbackLockReleaseFromMultiDc(10, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentCancelForceEndFromMultiDc() throws Exception {
        testConcurrentCancelLockReleaseFromMultiDc(10, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForcePublishForceEndFromMultiDc() throws Exception {
        testConcurrentForcePublishLockReleaseFromMultiDc(10, 10);
    }

    @Test
    @DirtiesContext
    public void testConcurrentForceEndForceEndFromMultiDc() throws Exception {
        testConcurrentForceEndLockReleaseFromMultiDc(10, 10);
    }


    // internal functions

    private void testConcurrentProcessExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentProcessCluster(clusterId, concurrent);

        Map<Long, MigrationCluster> migrationClusters = event.getMigrationClustersMap();
        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).start();
        checkAllMigrationOver(migrationClusters);
        checkLockRelease();
    }

    private void testConcurrentProcessLockReleaseFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentProcessClusterFromMultiDc(clusterId, concurrent);
        checkMultiDcMigrationExecAndOver(clusterId);
    }

    private void testConcurrentRollbackExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentRollback(clusterId, concurrent);

        Map<Long, MigrationCluster> migrationClusters = event.getMigrationClustersMap();
        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).rollback();
        checkAllMigrationOver(migrationClusters);
        checkLockRelease();
    }

    private void testConcurrentRollbackLockReleaseFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentRollbackFromMultiDc(clusterId, concurrent);
        checkMultiDcMigrationExecAndOver(clusterId);
    }

    private void testConcurrentCancelExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentCancel(clusterId, concurrent);

        Map<Long, MigrationCluster> migrationClusters = event.getMigrationClustersMap();
        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).cancel();
        checkAllMigrationOver(migrationClusters);
        checkLockRelease();
    }

    private void testConcurrentCancelLockReleaseFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentCancelFromMultiDc(clusterId, concurrent);
        checkMultiDcMigrationExecAndOver(clusterId);
    }

    private void testConcurrentForcePublishExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentForcePublish(clusterId, concurrent);

        Map<Long, MigrationCluster> migrationClusters = event.getMigrationClustersMap();
        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).forceProcess();
        checkAllMigrationOver(migrationClusters);
        checkLockRelease();
    }

    private void testConcurrentForcePublishLockReleaseFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentForcePublishFromMultiDc(clusterId, concurrent);
        checkMultiDcMigrationExecAndOver(clusterId);
    }

    private void testConcurrentForceEndExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentForceEnd(clusterId, concurrent);

        Map<Long, MigrationCluster> migrationClusters = event.getMigrationClustersMap();
        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).forceEnd();
        checkAllMigrationOver(migrationClusters);
        checkLockRelease();
    }

    private void testConcurrentForceEndLockReleaseFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentForceEndFromMultiDc(clusterId, concurrent);
        checkMultiDcMigrationExecAndOver(clusterId);
    }

    private void checkMultiDcMigrationExecAndOver(long clusterId) throws Exception {
        Map<Long, MigrationCluster> migrationClusters = event.getMigrationClustersMap();
        Map<Long, MigrationCluster> otherMigrationClusters = otherEvent.getMigrationClustersMap();

        MigrationCluster cluster = migrationClusters.get(clusterId);
        MigrationCluster otherCluster = otherMigrationClusters.get(clusterId);
        Assert.assertTrue((Mockito.mockingDetails(cluster).getInvocations().stream().anyMatch(invocation -> invocation.getMethod().getName().equals("allowStart")))
                ^ (Mockito.mockingDetails(otherCluster).getInvocations().stream().anyMatch(invocation -> invocation.getMethod().getName().equals("allowStart"))));

        checkAllMigrationOver(migrationClusters);
        checkAllMigrationOver(otherMigrationClusters);
        checkLockRelease();
    }

    private void checkAllMigrationOver(Map<Long, MigrationCluster> migrationClusters) {
        migrationClusters.values().forEach(migrationCluster -> {
            logger.info("[checkAllMigrationOver] {}-{}", migrationCluster, ((DefaultMigrationCluster) migrationCluster).getMigrationState().getStateActionState());
            Assert.assertTrue(!migrationCluster.isStarted() || migrationCluster.getStatus().isTerminated());
            Assert.assertFalse(((DefaultMigrationCluster) migrationCluster).getMigrationState().getStateActionState() instanceof Doing);
        });
    }

    private void checkLockRelease() throws Exception {
        Assert.assertEquals("", getExecLock());
    }

    private void concurrentProcessCluster(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::continueMigrationCluster);
    }

    private void concurrentProcessClusterFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigrationForMultiDc(clusterId, concurrent, DefaultMigrationEvent::processCluster);
    }

    private void concurrentRollback(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::rollbackMigrationCluster);
    }

    private void concurrentRollbackFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigrationForMultiDc(clusterId, concurrent, DefaultMigrationEvent::rollbackCluster);
    }

    private void concurrentCancel(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::cancelMigrationCluster);
    }

    private void concurrentCancelFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigrationForMultiDc(clusterId, concurrent, DefaultMigrationEvent::cancelCluster);
    }

    private void concurrentForcePublish(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::forceProcessMigrationCluster);
    }

    private void concurrentForcePublishFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigrationForMultiDc(clusterId, concurrent, DefaultMigrationEvent::forceClusterProcess);
    }

    private void concurrentForceEnd(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::forceEndMigrationClsuter);
    }

    private void concurrentForceEndFromMultiDc(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigrationForMultiDc(clusterId, concurrent, DefaultMigrationEvent::forceClusterEnd);
    }

    private void concurrentHandleMigrationForMultiDc(long clusterId, int concurrent, MigrationEventHandler eventHandler) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(concurrent);
        CountDownLatch countDownLatch = new CountDownLatch(concurrent);

        IntStream.range(0, concurrent).forEach(i -> {
            executors.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    logger.info("[concurrentHandleMigrationForMultiDc] barrier fail", e);
                }

                try {
                    if (0 == i % 2) eventHandler.handle(event, clusterId);
                    else eventHandler.handle(otherEvent, clusterId);
                } catch (Exception e) {
                    logger.info("[concurrentHandleMigrationForMultiDc] handle fail", e);
                }
                countDownLatch.countDown();
            });
        });

        countDownLatch.await(5000, TimeUnit.MILLISECONDS);
        waitConditionUntilTimeOut(() -> !event.isRunning(), 5000);
        waitConditionUntilTimeOut(() -> !otherEvent.isRunning(), 5000);
    }

    private void concurrentHandleMigration(long clusterId, int concurrent, MigrationHandler handler) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(concurrent);
        CountDownLatch countDownLatch = new CountDownLatch(concurrent);

        IntStream.range(0, concurrent).forEach(i -> {
            executors.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    logger.info("[concurrentHandleMigration] barrier fail", e);
                }

                try {
                    handler.handle(1, clusterId);
                } catch (Exception e) {

                }
                countDownLatch.countDown();
            });
        });

        countDownLatch.await(5000, TimeUnit.MILLISECONDS);
        waitConditionUntilTimeOut(() -> !event.isRunning(), 5000);
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-lock-test.sql");
    }

    private String getExecLock() throws Exception {
        MigrationEventTbl eventTbl = migrationEventTblDao.findByPK(1, MigrationEventTblEntity.READSET_FULL);
        return eventTbl.getExecLock();
    }

    private Map<Long, MigrationCluster> hackMigrationClusters(DefaultMigrationEvent migrationEvent) {
        Map<Long, MigrationCluster> origin = migrationEvent.getMigrationClustersMap();
        Map<Long, MigrationCluster> migrationClusters = new HashMap<>();
        origin.forEach((id, cluster) -> migrationClusters.put(id, Mockito.spy(cluster)));
        migrationEvent.setMigrationClustersMap(migrationClusters);

        return origin;
    }

    private void restoreMigrationClusters() {
        event.setMigrationClustersMap(originClusters);
        otherEvent.setMigrationClustersMap(originClustersForOtherDc);
    }


    @FunctionalInterface
    private interface MigrationHandler {
        void handle(long eventId, long clusterId) throws Exception;
    }

    @FunctionalInterface
    private interface MigrationEventHandler {
        void handle(DefaultMigrationEvent migrationEvent, long cluster) throws Exception;
    }

}
