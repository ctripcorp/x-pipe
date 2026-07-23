package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterActiveDcNotRequest;
import com.ctrip.xpipe.redis.console.service.migration.exception.ToIdcNotFoundException;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationServiceImplTryMigrateHeteroTest {

    @Mock
    private ClusterService clusterService;
    @Mock
    private DcService dcService;
    @Mock
    private DcCache dcCache;
    @Mock
    private HeteroMigrationSupport heteroMigrationSupport;
    @Mock
    private MigrationSystemAvailableChecker checker;
    @Mock
    private ConfigService configService;
    @Mock
    private MigrationClusterDao migrationClusterDao;
    @Mock
    private DcRelationsService dcRelationsService;

    private MigrationServiceImpl migrationService;

    private ClusterTbl heteroCluster;
    private AzGroupClusterEntity sgpOneWayAzGroup;
    private DcTbl jqDc;
    private DcTbl oyDc;
    private DcTbl uatDc;
    private DcTbl fraDc;

    @Before
    public void setUp() {
        migrationService = new MigrationServiceImpl()
                .setClusterService(clusterService)
                .setDcService(dcService)
                .setDcCache(dcCache)
                .setHeteroMigrationSupport(heteroMigrationSupport)
                .setConfigService(configService)
                .setMigrationClusterDao(migrationClusterDao);
        migrationService.setChecker(checker);
        migrationService.setDcRelationsService(dcRelationsService);

        lenient().when(checker.getResult()).thenReturn(MigrationSystemAvailableChecker.MigrationSystemAvailability.createAvailableResponse());
        lenient().when(configService.ignoreMigrationSystemAvailability()).thenReturn(false);
        lenient().when(migrationClusterDao.findUnfinishedByClusterId(anyLong())).thenReturn(Collections.emptyList());

        heteroCluster = new ClusterTbl().setId(14L).setClusterName("hetero-dual-oneway")
                .setClusterType(ClusterType.HETERO.name()).setActivedcId(1L);
        sgpOneWayAzGroup = new AzGroupClusterEntity().setId(24L).setActiveAzId(3L);
        jqDc = new DcTbl().setId(1L).setDcName("jq").setZoneId(1L);
        oyDc = new DcTbl().setId(2L).setDcName("oy").setZoneId(1L);
        uatDc = new DcTbl().setId(3L).setDcName("uat").setZoneId(2L);
        fraDc = new DcTbl().setId(4L).setDcName("fra").setZoneId(2L);

        when(clusterService.find("hetero-dual-oneway")).thenReturn(heteroCluster);
        when(heteroMigrationSupport.isHeteroCluster(heteroCluster)).thenReturn(true);
        when(dcCache.find(3L)).thenReturn(uatDc);
        when(heteroMigrationSupport.resolveMigrationAzGroupClusters(
                eq(Collections.singletonList(14L)), eq("uat"))).thenReturn(Collections.singletonMap(14L, sgpOneWayAzGroup));
        when(dcService.findClusterRelatedDc("hetero-dual-oneway"))
                .thenReturn(Arrays.asList(jqDc, oyDc, uatDc, fraDc));
        when(heteroMigrationSupport.isSameAzGroup(14L, "uat", "fra")).thenReturn(true);
        when(heteroMigrationSupport.isSameAzGroup(14L, "uat", "jq")).thenReturn(false);
    }

    @Test
    public void heteroTryMigrateShouldUseAzGroupActiveInsteadOfClusterTbl() throws Exception {
        TryMigrateResult result = migrationService.tryMigrate("hetero-dual-oneway", "uat", "fra");

        Assert.assertEquals("hetero-dual-oneway", result.getClusterName());
        Assert.assertEquals(3L, result.getFromDcId());
        Assert.assertEquals("uat", result.getFromDcName());
        Assert.assertEquals(4L, result.getToDcId());
        Assert.assertEquals("fra", result.getToDcName());
    }

    @Test(expected = ClusterActiveDcNotRequest.class)
    public void heteroTryMigrateShouldRejectWhenFromIdcIsNotAzGroupActive() throws Exception {
        when(heteroMigrationSupport.resolveMigrationAzGroupClusters(
                eq(Collections.singletonList(14L)), eq("fra"))).thenReturn(Collections.singletonMap(14L, sgpOneWayAzGroup));

        migrationService.tryMigrate("hetero-dual-oneway", "fra", "uat");
    }

    @Test(expected = ClusterActiveDcNotRequest.class)
    public void heteroTryMigrateShouldRejectWhenFromIdcMissing() throws Exception {
        migrationService.tryMigrate("hetero-dual-oneway", null, "fra");
    }

    @Test(expected = ToIdcNotFoundException.class)
    public void heteroFindToDcShouldRejectCrossAzGroupTarget() throws Exception {
        List<DcTbl> relatedDcs = new LinkedList<>(Arrays.asList(jqDc, oyDc, uatDc, fraDc));
        migrationService.findToDc(heteroCluster, "uat", "jq", relatedDcs, sgpOneWayAzGroup);
    }

    @Test
    public void heteroFindToDcShouldAllowSameAzGroupTarget() throws Exception {
        List<DcTbl> relatedDcs = new LinkedList<>(Arrays.asList(jqDc, oyDc, uatDc, fraDc));
        DcTbl toDc = migrationService.findToDc(heteroCluster, "uat", "fra", relatedDcs, sgpOneWayAzGroup);
        Assert.assertEquals("fra", toDc.getDcName());
    }
}
