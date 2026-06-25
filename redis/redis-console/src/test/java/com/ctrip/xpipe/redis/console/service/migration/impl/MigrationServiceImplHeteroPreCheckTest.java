package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationServiceImplHeteroPreCheckTest {

    @Mock
    private ClusterService clusterService;
    @Mock
    private DcClusterService dcClusterService;
    @Mock
    private DcCache dcCache;
    @Mock
    private HeteroMigrationSupport heteroMigrationSupport;
    @Mock
    private MigrationEventDao migrationEventDao;
    @Mock
    private MigrationEventManager migrationEventManager;
    @Mock
    private MigrationEvent migrationEvent;

    private MigrationServiceImpl migrationService;
    private ClusterTbl clusterTbl;
    private AzGroupClusterEntity shaOneWayAzGroup;
    private DcTbl jqDc;
    private DcTbl oyDc;

    @Before
    public void setUp() {
        migrationService = new MigrationServiceImpl()
                .setClusterService(clusterService)
                .setDcClusterService(dcClusterService)
                .setDcCache(dcCache)
                .setHeteroMigrationSupport(heteroMigrationSupport)
                .setMigrationEventDao(migrationEventDao)
                .setMigrationEventManager(migrationEventManager);

        clusterTbl = new ClusterTbl().setId(14L).setClusterName("hetero-dual-oneway")
                .setClusterType(ClusterType.HETERO.name());
        shaOneWayAzGroup = new AzGroupClusterEntity().setId(23L).setActiveAzId(1L);
        jqDc = new DcTbl().setId(1L).setDcName("jq");
        oyDc = new DcTbl().setId(2L).setDcName("oy");

        when(clusterService.find(14L)).thenReturn(clusterTbl);
        when(heteroMigrationSupport.isHeteroCluster(clusterTbl)).thenReturn(true);
        when(dcClusterService.find(1L, 14L)).thenReturn(new DcClusterTbl());
        when(dcClusterService.find(2L, 14L)).thenReturn(new DcClusterTbl());
        when(dcCache.find(1L)).thenReturn(jqDc);
        when(dcCache.find(2L)).thenReturn(oyDc);
        when(heteroMigrationSupport.resolveMigrationAzGroupClusters(
                eq(Collections.singletonList(14L)), eq("jq"))).thenReturn(Collections.singletonMap(14L, shaOneWayAzGroup));
    }

    @Test
    public void heteroPreCheckShouldPassWithCorrectSourceDc() {
        when(heteroMigrationSupport.isSameAzGroup(14L, "jq", "oy")).thenReturn(true);
        when(migrationEventDao.createMigrationEvent(any(MigrationRequest.class))).thenReturn(migrationEvent);
        when(migrationEvent.getEvent()).thenReturn(new MigrationEventTbl().setId(100L));

        Long eventId = migrationService.createMigrationEvent(buildRequest(1L, 2L, 23L));
        Assert.assertEquals(Long.valueOf(100L), eventId);
        verify(migrationEventDao).createMigrationEvent(any(MigrationRequest.class));
        verify(migrationEventManager).addEvent(migrationEvent);
    }

    @Test(expected = BadRequestException.class)
    public void heteroPreCheckShouldRejectWrongSourceDc() {
        when(heteroMigrationSupport.resolveMigrationAzGroupClusters(
                eq(Collections.singletonList(14L)), eq("oy"))).thenReturn(Collections.singletonMap(14L, shaOneWayAzGroup));
        migrationService.createMigrationEvent(buildRequest(2L, 1L, null));
    }

    @Test(expected = BadRequestException.class)
    public void heteroPreCheckShouldRejectIllegalSourceDcId() {
        when(dcCache.find(99L)).thenReturn(null);
        migrationService.createMigrationEvent(buildRequest(99L, 2L, null));
    }

    @Test(expected = BadRequestException.class)
    public void heteroPreCheckShouldRejectUnresolvedAzGroup() {
        when(heteroMigrationSupport.resolveMigrationAzGroupClusters(
                eq(Collections.singletonList(14L)), eq("jq"))).thenReturn(Collections.emptyMap());
        migrationService.createMigrationEvent(buildRequest(1L, 2L, null));
    }

    @Test(expected = BadRequestException.class)
    public void heteroPreCheckShouldRejectIllegalTargetDcId() {
        when(dcClusterService.find(99L, 14L)).thenReturn(new DcClusterTbl());
        when(dcCache.find(99L)).thenReturn(null);
        migrationService.createMigrationEvent(buildRequest(1L, 99L, null));
    }

    @Test(expected = BadRequestException.class)
    public void heteroPreCheckShouldRejectCrossAzGroupTarget() {
        when(heteroMigrationSupport.isSameAzGroup(14L, "jq", "oy")).thenReturn(false);
        migrationService.createMigrationEvent(buildRequest(1L, 2L, null));
    }

    @Test(expected = BadRequestException.class)
    public void heteroPreCheckShouldRejectMismatchedAzGroupClusterId() {
        migrationService.createMigrationEvent(buildRequest(1L, 2L, 99L));
    }

    @Test(expected = BadRequestException.class)
    public void heteroPreCheckShouldRejectNullActiveAzId() {
        AzGroupClusterEntity azGroupWithoutActive = new AzGroupClusterEntity().setId(23L).setActiveAzId(null);
        when(heteroMigrationSupport.resolveMigrationAzGroupClusters(
                eq(Collections.singletonList(14L)), eq("jq"))).thenReturn(Collections.singletonMap(14L, azGroupWithoutActive));
        migrationService.createMigrationEvent(buildRequest(1L, 2L, null));
    }

    private MigrationRequest buildRequest(long fromDcId, long toDcId, Long azGroupClusterId) {
        MigrationRequest request = new MigrationRequest("test-user");
        MigrationRequest.ClusterInfo clusterInfo = new MigrationRequest.ClusterInfo();
        clusterInfo.setClusterId(14L);
        clusterInfo.setFromDcId(fromDcId);
        clusterInfo.setToDcId(toDcId);
        clusterInfo.setAzGroupClusterId(azGroupClusterId);
        request.addClusterInfo(clusterInfo);
        return request;
    }
}
