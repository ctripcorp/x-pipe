package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationMigratingState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishState;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2018
 */
public class DefaultMigrationClusterTest2 {

    @Mock
    private DefaultMigrationCluster migrationCluster;

    @Mock
    private MigrationEvent migrationEvent;

    private ExecutorService executors = Executors.newFixedThreadPool(1);

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    @Mock
    private ClusterService clusterService;
    @Mock
    private ShardService shardService;
    @Mock
    private DcService dcService;
    @Mock
    private RedisService redisService;
    @Mock
    private MigrationService migrationService;

    private ClusterTbl clusterTbl = new ClusterTbl();

    private MigrationClusterTbl migrationClusterTbl = new MigrationClusterTbl();

    @Before
    public void beforeDefaultMigrationClusterTest2() {

        MockitoAnnotations.initMocks(this);
        clusterTbl.setClusterName("cluster-1").setStatus("Normal");

        migrationClusterTbl.setCluster(clusterTbl).setClusterId(1L).setStatus("Initiated");

        when(clusterService.find(1L)).thenReturn(clusterTbl);
        when(shardService.findAllByClusterName(any())).thenReturn(Collections.emptyList());
        when(dcService.findClusterRelatedDc(any())).thenReturn(Collections.emptyList());


        migrationCluster = new DefaultMigrationCluster(executors, scheduled, migrationEvent, migrationClusterTbl,
                dcService, clusterService, shardService, redisService, migrationService);
    }

    @Test
    public void testUpdateStorageClusterStatus() throws Exception {

        AtomicInteger counter = new AtomicInteger(1);
        when(clusterService.find(anyString())).thenReturn(clusterTbl);
        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                int currentCounter = counter.getAndIncrement();
                if(currentCounter % 3 == 0) {
                    clusterTbl.setStatus(((ClusterStatus) invocation.getArguments()[1]).toString());
                }
                return null;
            }
        }).when(clusterService).updateStatusById(anyLong(), any());

        migrationClusterTbl.setStatus("Checking");
        migrationCluster.updateStorageClusterStatus();

        Assert.assertEquals(MigrationStatus.Checking.getClusterStatus().toString(), clusterTbl.getStatus());
    }

    @Test(expected = ServerException.class)
    public void testUpdateStat() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        when(clusterService.find(anyString())).thenReturn(clusterTbl);
        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                int currentCounter = counter.getAndIncrement();
                if(currentCounter % 5 == 0) {
                    clusterTbl.setStatus(((ClusterStatus) invocation.getArguments()[1]).toString());
                }
                return null;
            }
        }).when(clusterService).updateStatusById(anyLong(), any());

        migrationCluster.updateStat(new MigrationCheckingState(migrationCluster));

    }

    @Test(expected = ServerException.class)
    public void testUpdateStat2() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        when(clusterService.find(anyString())).thenReturn(clusterTbl);
        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                int currentCounter = counter.getAndIncrement();
                if(currentCounter % 9 == 0) {
                    clusterTbl.setStatus(((ClusterStatus) invocation.getArguments()[1]).toString());
                }
                return null;
            }
        }).when(clusterService).updateStatusById(anyLong(), any());

        migrationCluster.updateStat(new MigrationMigratingState(migrationCluster));

    }

    @Test(expected = ServerException.class)
    public void testUpdateStat3() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        when(clusterService.find(anyString())).thenReturn(clusterTbl);
        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                int currentCounter = counter.getAndIncrement();
                if(currentCounter % 7 == 0) {
                    clusterTbl.setStatus(((ClusterStatus) invocation.getArguments()[1]).toString());
                }
                return null;
            }
        }).when(clusterService).updateStatusById(anyLong(), any());

        migrationCluster.updateStat(new MigrationPublishState(migrationCluster));

    }

}
