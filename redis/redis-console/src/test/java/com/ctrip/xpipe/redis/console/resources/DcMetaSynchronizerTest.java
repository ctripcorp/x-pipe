package com.ctrip.xpipe.redis.console.resources;


import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DcMetaSynchronizerTest {

    @InjectMocks
    @Spy
    private DcMetaSynchronizer dcMetaSynchronizer;

    @Mock
    private OrganizationService organizationService;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private MetaCache metaCache;

    @Mock
    private RedisService redisService;

    @Mock
    private ShardService shardService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private DcService dcService;

    @Mock
    private OuterClientService outerClientService;

    private String singleDcCacheCluster = "SingleDcCacheCluster";
    private String localDcCacheCluster = "LocalDcCacheCluster";

    @Test
    public void syncTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(18L).setOrgId(56).setOrgName("平台"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(outerClientService.getOutClientDcMeta(DcMetaSynchronizer.currentDcId)).thenReturn(credisDcMeta().setDcName(DcMetaSynchronizer.currentDcId));
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(DcMetaSynchronizer.currentDcId)));
        when(dcService.find(DcMetaSynchronizer.currentDcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(OuterClientService.ClusterType.SINGEL_DC.name()).setClusterAdminEmails("lilj@ctrip.com").setClusterOrgId(18));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(OuterClientService.ClusterType.SINGEL_DC.name()).setClusterAdminEmails("lilj@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();
    }

    OuterClientService.DcMeta credisDcMeta() {
        return JsonUtil.fromJson(JsonUtil.credisMetaString, OuterClientService.DcMeta.class);
    }

    DcMeta xpipeDcMeta() {
        return JsonUtil.fromJson(JsonUtil.xpipeDcMeta, DcMeta.class);
    }
}
