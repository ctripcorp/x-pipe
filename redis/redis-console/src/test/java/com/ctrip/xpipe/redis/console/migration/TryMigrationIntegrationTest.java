package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TryMigrationIntegrationTest extends AbstractMigrationIntegrationTest {

    @Before
    public void beforeConsoleMigrationIntegrationTest() {

        when(clusterService.find(anyString())).thenAnswer(new Answer<ClusterTbl>() {
            @Override
            public ClusterTbl answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(randomInt(5, 25));
                return new ClusterTbl().setId(10000L).setActivedcId(100L)
                        .setClusterType(ClusterType.ONE_WAY.toString());
            }
        });
        when(migrationClusterDao.findUnfinishedByClusterId(anyLong()))
                .thenAnswer(new Answer<List<MigrationClusterTbl>>() {
                    @Override
                    public List<MigrationClusterTbl> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Thread.sleep(randomInt(4, 29));
                        return Lists.newArrayListWithCapacity(1);
                    }
                });
        when(dcService.find(anyLong())).thenAnswer(new Answer<DcTbl>() {
            @Override
            public DcTbl answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(randomInt(2, 7));
                return new DcTbl().setDcName(fromIdc).setZoneId(1L);
            }
        });
        when(dcService.findClusterRelatedDc(anyString())).thenAnswer(new Answer<List<DcTbl>>() {
            @Override
            public List<DcTbl> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(randomInt(5, 29));
                return Lists.newArrayList(new DcTbl().setDcName(fromIdc).setZoneId(1L),
                        new DcTbl().setDcName(toIdc).setZoneId(1L));
            }
        });
    }

    @Test(expected = MigrationNotSupportException.class)
    public void tryMigrate() throws Exception {
        when(clusterService.find(anyString())).thenAnswer(new Answer<ClusterTbl>() {
            @Override
            public ClusterTbl answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(randomInt(5, 25));
                return new ClusterTbl().setId(10000L).setActivedcId(100L)
                        .setClusterType(ClusterType.BI_DIRECTION.toString());
            }
        });

        migrationService.tryMigrate("cluster", fromIdc, toIdc);
    }

    @Test
    public void testConsoleTryMigration()
            throws ClusterMigratingNow, ToIdcNotFoundException, ClusterNotFoundException, MigrationNotSupportException,
            MigrationSystemNotHealthyException, ClusterActiveDcNotRequest, ClusterMigratingNowButMisMatch {

        migrationService.tryMigrate("cluster-", fromIdc, toIdc);
    }

    @Test
    public void testBatchTryMigration() throws Exception {
        int batch = 3000;
        ExecutorService executors = Executors.newFixedThreadPool(200);
        AtomicBoolean success = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(batch);
        for (int i = 0; i < batch; i++) {
            int finalI = i;
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        migrationService.tryMigrate("cluster-" + finalI, fromIdc, toIdc);
                        latch.countDown();
                    } catch (Exception e) {
                        success.set(false);
                        logger.error("", e);
                    }
                }
            });
        }
        latch.await(860, TimeUnit.MILLISECONDS);
        Assert.assertTrue(success.get());
    }

    @Test
    public void testTryManyTimes() throws Exception {
        int NTimes = 10;
        for (int k = 0; k < NTimes; k++) {
            int batch = 3000;
            ExecutorService executors = Executors.newFixedThreadPool(200);
            AtomicBoolean success = new AtomicBoolean(true);
            CountDownLatch latch = new CountDownLatch(batch);
            for (int i = 0; i < batch; i++) {
                int finalI = i;
                executors.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            migrationService.tryMigrate("cluster-" + finalI, fromIdc, toIdc);
                            latch.countDown();
                        } catch (Exception e) {
                            success.set(false);
                            logger.error("", e);
                        }
                    }
                });
            }
            latch.await(860, TimeUnit.MILLISECONDS);
            Assert.assertTrue(success.get());
        }
    }

    @Test
    public void testTryHard() throws Exception {

        int batch = 10000;
        ExecutorService executors = Executors.newFixedThreadPool(200);
        AtomicBoolean success = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(batch);
        for (int i = 0; i < batch; i++) {
            int finalI = i;
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        migrationService.tryMigrate("cluster-" + finalI, fromIdc, toIdc);
                        latch.countDown();
                    } catch (Exception e) {
                        success.set(false);
                        logger.error("", e);
                    }
                }
            });
        }
        latch.await(2880, TimeUnit.MILLISECONDS);
        Assert.assertTrue(success.get());

    }
}
