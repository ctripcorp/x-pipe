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

    private Map<Long, MigrationCluster> migrationClusters;

    private MigrationEventTblDao migrationEventTblDao;

    @Before
    public void setupMigrationExecuteLockTest() throws Exception {
        migrationEventManager = migrationService.getMigrationEventManager();
        event = (DefaultMigrationEvent) migrationEventManager.getEvent(1);
        hackMigrationClusters();
        otherEvent = (DefaultMigrationEvent) migrationEventDao.buildMigrationEvent(1);

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

    private void testConcurrentProcessExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentProcessCluster(clusterId, concurrent);

        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).start();
        checkAllMigrationOver();
        Assert.assertEquals("", getExecLock());
    }

    private void testConcurrentRollbackExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentRollback(clusterId, concurrent);

        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).rollback();
        checkAllMigrationOver();
        Assert.assertEquals("", getExecLock());
    }

    private void testConcurrentCancelExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentCancel(clusterId, concurrent);

        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).cancel();
        checkAllMigrationOver();
        Assert.assertEquals("", getExecLock());
    }

    private void testConcurrentForcePublishExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentForcePublish(clusterId, concurrent);

        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).forcePublish();
        checkAllMigrationOver();
        Assert.assertEquals("", getExecLock());
    }

    private void testConcurrentForceEndExecuteOneTimeAndLockRelease(long clusterId, int concurrent) throws Exception {
        concurrentForceEnd(clusterId, concurrent);

        MigrationCluster cluster = migrationClusters.get(clusterId);
        Mockito.verify(cluster, Mockito.atLeast(1)).forceEnd();
        checkAllMigrationOver();
        Assert.assertEquals("", getExecLock());
    }

    private void checkAllMigrationOver() {
        migrationClusters.values().forEach(migrationCluster -> {
            logger.info("[checkAllMigrationOver] {}-{}", migrationCluster, ((DefaultMigrationCluster) migrationCluster).getMigrationState().getStateActionState());
            Assert.assertTrue(!migrationCluster.isStarted() || migrationCluster.getStatus().isTerminated());
            Assert.assertFalse(((DefaultMigrationCluster) migrationCluster).getMigrationState().getStateActionState() instanceof Doing);
        });
    }

    private void concurrentProcessCluster(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::continueMigrationCluster);
    }

    private void concurrentRollback(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::rollbackMigrationCluster);
    }

    private void concurrentCancel(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::cancelMigrationCluster);
    }

    private void concurrentForcePublish(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::forcePublishMigrationCluster);
    }

    private void concurrentForceEnd(long clusterId, int concurrent) throws Exception {
        concurrentHandleMigration(clusterId, concurrent, migrationService::forceEndMigrationClsuter);
    }

    private void concurrentHandleMigration(long clusterId, int concurrent, MigrationHandler handler) throws Exception {
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

    private void hackMigrationClusters() {
        originClusters = event.getMigrationClustersMap();
        migrationClusters = new HashMap<>();
        originClusters.forEach((id, cluster) -> migrationClusters.put(id, Mockito.spy(cluster)));
        event.setMigrationClustersMap(migrationClusters);
    }

    private void restoreMigrationClusters() {
        event.setMigrationClustersMap(originClusters);
    }


    @FunctionalInterface
    private interface MigrationHandler {
        void handle(long eventId, long clusterId) throws Exception;
    }

}
