package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClusterMetaServiceImplHeteroMigrationTest {

    @Mock
    private MigrationService migrationService;

    private ClusterMetaServiceImpl clusterMetaService;

    private ClusterTbl clusterTbl;

    private AzGroupClusterEntity shaOneWayAzGroup;

    @Before
    public void setUp() {
        clusterMetaService = new ClusterMetaServiceImpl();
        clusterMetaService.setMigrationService(migrationService);

        clusterTbl = new ClusterTbl();
        clusterTbl.setId(14L);
        clusterTbl.setClusterName("hetero-dual-oneway");
        clusterTbl.setStatus(ClusterStatus.Migrating.toString());
        clusterTbl.setMigrationEventId(100L);

        shaOneWayAzGroup = new AzGroupClusterEntity();
        shaOneWayAzGroup.setActiveAzId(1L);
        shaOneWayAzGroup.setAzGroupClusterType(ClusterType.ONE_WAY.toString());

        when(migrationService.findMigrationCluster(100L, 14L))
                .thenReturn(new MigrationClusterTbl().setDestinationDcId(2L));
    }

    @Test
    public void destinationDcShouldBeActiveDuringMigration() {
        DcTbl oy = new DcTbl().setId(2L).setDcName("oy");
        Assert.assertEquals(2L, clusterMetaService.getAzGroupClusterMetaCurrentPrimaryDc(oy, clusterTbl, shaOneWayAzGroup));
    }

    @Test
    public void sourceDcShouldKeepOriginActiveAzDuringMigration() {
        DcTbl jq = new DcTbl().setId(1L).setDcName("jq");
        Assert.assertEquals(1L, clusterMetaService.getAzGroupClusterMetaCurrentPrimaryDc(jq, clusterTbl, shaOneWayAzGroup));
    }

    @Test
    public void singleDcAzGroupShouldIgnoreMigrationOverride() {
        AzGroupClusterEntity singleDcAzGroup = new AzGroupClusterEntity();
        singleDcAzGroup.setActiveAzId(3L);
        singleDcAzGroup.setAzGroupClusterType(ClusterType.SINGLE_DC.toString());

        DcTbl fra = new DcTbl().setId(3L).setDcName("fra");
        Assert.assertEquals(3L, clusterMetaService.getAzGroupClusterMetaCurrentPrimaryDc(fra, clusterTbl, singleDcAzGroup));
    }
}
