package com.ctrip.xpipe.redis.console.resources;


import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterTypeUpdateEventFactory;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DcMetaSynchronizerTest {

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

    @Mock
    private SentinelBalanceService sentinelBalanceService;

    @Mock
    private ClusterTypeUpdateEventFactory clusterTypeUpdateEventFactory;

    private String singleDcCacheCluster = "SingleDcCacheCluster";
    private String localDcCacheCluster = "LocalDcCacheCluster";

    private String dcId = FoundationService.DEFAULT.getDataCenter();

    @Before
    public void beforeDcMetaSynchronizerTest() {
        dcMetaSynchronizer = new DcMetaSynchronizer(consoleConfig, metaCache, redisService, shardService,
                clusterService, dcService, organizationService, sentinelBalanceService,
                clusterTypeUpdateEventFactory, outerClientService, dcId);
    }

    @Test
    public void syncNoChangedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));
        OuterClientService. DcMeta meta =  credisDcMeta().setDcName(dcId);
        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(meta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(anyString())).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(OuterClientService.ClusterType.SINGEL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(18));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(OuterClientService.ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, never()).deleteRedises(any(), any(), any(), any());
        verify(redisService, never()).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
    }

    @Test
    public void syncClusterActiveDcChangedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta().setDcName(dcId));

        DcMeta xpipeDcMeta = xpipeDcMeta().setId(dcId);
        xpipeDcMeta.findCluster(singleDcCacheCluster).setActiveDc("oy");

        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).
                setClusterName(singleDcCacheCluster).setActivedcId(2).setClusterType(ClusterType.SINGLE_DC.name()).
                setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).
                setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).
                setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, times(1)).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, never()).deleteRedises(any(), any(), any(), any());
        verify(redisService, never()).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
    }

    @Test
    public void syncClusterEmailsChangedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));

//        change admin emails
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        credisDcMeta.getClusters().get(singleDcCacheCluster).setOwnerEmails("test2@ctrip.com");

        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).
                setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).
                setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).
                setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).
                setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, times(1)).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, never()).deleteRedises(any(), any(), any(), any());
        verify(redisService, never()).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
    }

    @Test
    public void syncClusterNameChangedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));

//        change cluster name
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        OuterClientService.ClusterMeta oldSingle = credisDcMeta.getClusters().get(singleDcCacheCluster);
        credisDcMeta.getClusters().remove(singleDcCacheCluster);
        String newName = "newName";
        credisDcMeta.getClusters().put(newName, oldSingle.setName(newName));

        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        when(clusterService.getClusterRelatedDcs(singleDcCacheCluster)).thenReturn(Lists.newArrayList(new DcTbl().setDcName(credisDcMeta.getDcName())));
        when(clusterService.find(newName)).thenReturn(null);
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, times(1)).createSingleGroupCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, times(1)).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, times(1)).findOrCreateShardIfNotExist(any(), any(), eq(null), any());

        verify(redisService, times(1)).insertRedises(any(), any(), any(), any());
    }

    @Test
    public void singleDcToOneWayTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));

        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        DcMeta xpipeDcMeta=xpipeDcMeta().setId(dcId);
        xpipeDcMeta.removeCluster(singleDcCacheCluster);

        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.ONE_WAY.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService,times(1)).find(singleDcCacheCluster);
        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());

        verify(redisService, never()).insertRedises(any(), any(), any(), any());
    }

    @Test
    public void clusterExistedButNoShardsTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));

        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        DcMeta xpipeDcMeta=xpipeDcMeta().setId(dcId);
        xpipeDcMeta.removeCluster(singleDcCacheCluster);

        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(2).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService,times(1)).find(singleDcCacheCluster);
        verify(clusterService, times(1)).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, times(1)).findOrCreateShardIfNotExist(any(), any(), eq(null), any());

        verify(redisService, times(1)).insertRedises(any(), any(), any(), any());
    }

    @Test
    public void syncClusterClusterTypeChangedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));

//        change cluster type
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        credisDcMeta.getClusters().get(singleDcCacheCluster).setClusterType(OuterClientService.ClusterType.LOCAL_DC);

        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, times(1)).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, never()).deleteRedises(any(), any(), any(), any());
        verify(redisService, never()).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
        verify(clusterTypeUpdateEventFactory,times(1)).createClusterEvent(anyString(),any(ClusterTbl.class));
    }

    @Test
    public void syncShardAddedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        //        add shard
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        credisDcMeta.getClusters().get(singleDcCacheCluster).getGroups().put("newGroup", new OuterClientService.GroupMeta().
                setClusterName(singleDcCacheCluster).setGroupName("newGroup").
                setRedises(Lists.newArrayList(new OuterClientService.RedisMeta().setHost("127.0.0.1").setPort(6379).setMaster(true))));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(consoleConfig.supportSentinelHealthCheck(any(),any())).thenReturn(false);
        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(consoleConfig, times(1)).supportSentinelHealthCheck(any(), any());
        verify(sentinelBalanceService, never()).selectMultiDcSentinels(ClusterType.ONE_WAY, "");
        verify(shardService, times(1)).findOrCreateShardIfNotExist(any(), any(), eq(null), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, never()).deleteRedises(any(), any(), any(), any());
        verify(redisService, times(1)).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());


        when(consoleConfig.supportSentinelHealthCheck(any(),any())).thenReturn(true);
        dcMetaSynchronizer.sync();
        verify(consoleConfig, times(2)).supportSentinelHealthCheck(any(), any());
        verify(sentinelBalanceService, times(1)).selectMultiDcSentinels(ClusterType.SINGLE_DC, "");
    }

    @Test
    public void syncShardRemovedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        //        rm shard
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        credisDcMeta.getClusters().get(singleDcCacheCluster).getGroups().clear();

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, times(1)).deleteShard(any(), any());

        verify(redisService, never()).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
    }

    @Test
    public void syncRedisAddedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        //        add redis
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        credisDcMeta.getClusters().get(singleDcCacheCluster).getGroups().get("credis_test_cluster_1_1").getRedises().add(new OuterClientService.RedisMeta().setHost("127.0.0.1").setPort(6379).setMaster(true));

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        when(redisService.findRedisesByDcClusterShard(dcId,singleDcCacheCluster,"credis_test_cluster_1_1")).thenReturn(Lists.newArrayList(new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(6379).setMaster(false)));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, never()).deleteRedises(any(), any(), any(), any());
        verify(redisService, times(1)).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
    }

    @Test
    public void syncRedisRemovedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        //        add redis
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);
        credisDcMeta.getClusters().get(singleDcCacheCluster).getGroups().get("credis_test_cluster_1_1").getRedises().clear();

        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, times(1)).deleteRedises(any(), any(), any(), any());
        verify(redisService, never()).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
    }

    @Test
    @Ignore
    public void syncRedisChangedTest() throws Exception {
        when(organizationService.getAllOrganizations()).thenReturn(Lists.newArrayList(
                new OrganizationTbl().setId(8L).setOrgId(44).setOrgName("框架"),
                new OrganizationTbl().setId(9L).setOrgId(45).setOrgName("酒店")
        ));

        //        change redis role
        OuterClientService.DcMeta credisDcMeta = credisDcMeta().setDcName(dcId);

        List<RedisTbl> redisTbls = new ArrayList<>();
        List<OuterClientService.RedisMeta> redisMetaList = credisDcMeta.getClusters().get(singleDcCacheCluster).getGroups().get("credis_test_cluster_1_1").getRedises();
        redisMetaList.forEach(redisMeta -> {
            redisTbls.add(new RedisTbl().setRedisIp(redisMeta.getHost()).setRedisPort(redisMeta.getPort()).setMaster(redisMeta.isMaster()));
            redisMeta.setMaster(!redisMeta.isMaster());
        });


        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC"));
        when(outerClientService.getOutClientDcMeta(dcId)).thenReturn(credisDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(xpipeDcMeta().setId(dcId)));
        when(dcService.find(dcId)).thenReturn(new DcTbl().setId(1));
        when(clusterService.find(singleDcCacheCluster)).thenReturn(new ClusterTbl().setId(17730).setClusterName(singleDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.SINGLE_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(8));
        when(clusterService.find(localDcCacheCluster)).thenReturn(new ClusterTbl().setId(17728).setClusterName(localDcCacheCluster).setActivedcId(1).setClusterType(ClusterType.LOCAL_DC.name()).setClusterAdminEmails("test@ctrip.com").setClusterOrgId(9));
        when(redisService.findRedisesByDcClusterShard(dcId, singleDcCacheCluster, "credis_test_cluster_1_1")).thenReturn(redisTbls);

        dcMetaSynchronizer.sync();

        verify(clusterService, never()).bindDc(any());
        verify(clusterService, never()).createCluster(any());
        verify(clusterService, never()).unbindDc(any(), any());
        verify(clusterService, never()).deleteCluster(any());
        verify(clusterService, never()).update(any());

        verify(shardService, never()).findOrCreateShardIfNotExist(any(), any(), anyList(), any());
        verify(shardService, never()).deleteShard(any(), any());

        verify(redisService, never()).deleteRedises(any(), any(), any(), any());
        verify(redisService, never()).insertRedises(any(), any(), any(), any());
        verify(redisService, never()).updateBatchMaster(any());
    }

    @Test
    public void shouldFilterOuterClusterTest() {

        OuterClientService.ClusterMeta clusterMeta = new OuterClientService.ClusterMeta().setName("testCluster").setOperating(false);
        Assert.assertFalse(dcMetaSynchronizer.shouldFilterOuterCluster(clusterMeta));
        Assert.assertFalse(dcMetaSynchronizer.shouldFilterOuterCluster(clusterMeta.setName("AddServCache_v202111011735")));
        Assert.assertFalse(dcMetaSynchronizer.shouldFilterOuterCluster(clusterMeta.setName("Ai_AdSystem_Cache_temp202103041704")));
        Assert.assertFalse(dcMetaSynchronizer.shouldFilterOuterCluster(clusterMeta.setName("Ai_AdSystem_Cache_v2")));

        clusterMeta.setOperating(true);
        Assert.assertTrue(dcMetaSynchronizer.shouldFilterOuterCluster(clusterMeta.setName("AddServCache_v202111011735")));
        Assert.assertTrue(dcMetaSynchronizer.shouldFilterOuterCluster(clusterMeta.setName("Ai_AdSystem_Cache_temp202103041704")));
        Assert.assertTrue(dcMetaSynchronizer.shouldFilterOuterCluster(clusterMeta.setName("Ai_AdSystem_Cache_v2")));

    }

    @Test
    public void shouldFilterInnerClusterTest() {
        Set<String> filteredOuterClusters = Sets.newHashSet("cluster1", "cluster2");
        Assert.assertTrue(dcMetaSynchronizer.shouldFilterInnerCluster(new ClusterMeta().setId("cluster2"),filteredOuterClusters));
        Assert.assertTrue(dcMetaSynchronizer.shouldFilterInnerCluster(new ClusterMeta().setId("cluster1"),filteredOuterClusters));
        Assert.assertFalse(dcMetaSynchronizer.shouldFilterInnerCluster(new ClusterMeta().setId("cluster"),filteredOuterClusters));
    }

    @Test
    public void extractOuterDcMetaWithInterestedTypesTest() {
        when(consoleConfig.getOuterClusterTypes()).thenReturn(Sets.newHashSet("SINGLE_DC", "LOCAL_DC", "CROSS_DC"));
        OuterClientService.DcMeta dcMeta = new OuterClientService.DcMeta();
        dcMeta.setDcName("SHAXY");
        Map<String, OuterClientService.ClusterMeta> clusters = new HashMap<>();
        OuterClientService.ClusterMeta xpipeCluster = new OuterClientService.ClusterMeta();
        xpipeCluster.setClusterType(OuterClientService.ClusterType.XPIPE_ONE_WAY);
        xpipeCluster.setOperating(true).setName("xpipeCluster").setOrgId(0).setOwnerEmails("test@ctrip.com");

        OuterClientService.ClusterMeta crossDcCluster = new OuterClientService.ClusterMeta();
        crossDcCluster.setClusterType(OuterClientService.ClusterType.TROCKS);
        crossDcCluster.setOperating(true).setName("crossDcCluster").setOrgId(0).setOwnerEmails("test@ctrip.com");

        OuterClientService.ClusterMeta singleDcCluster = new OuterClientService.ClusterMeta();
        singleDcCluster.setClusterType(OuterClientService.ClusterType.SINGEL_DC);
        singleDcCluster.setOperating(false).setName("singleDcCluster").setOrgId(0).setOwnerEmails("test@ctrip.com");

        clusters.put("xpipeCluster", xpipeCluster);
        clusters.put("crossDcCluster", crossDcCluster);
        clusters.put("singleDcCluster", singleDcCluster);
        dcMeta.setClusters(clusters);
        Pair<DcMeta, Set<String>> outerDcMeta = dcMetaSynchronizer.extractOuterDcMetaWithInterestedTypes(dcMeta);
        Assert.assertEquals(1, outerDcMeta.getKey().getClusters().size());
        Assert.assertTrue(outerDcMeta.getKey().getClusters().containsKey("singleDcCluster"));
        Assert.assertEquals(Sets.newHashSet("xpipeCluster", "crossDcCluster"), outerDcMeta.getValue());
    }

    OuterClientService.DcMeta credisDcMeta() {
        OuterClientService.DcMeta  res = JsonUtil.fromJson(JsonUtil.credisMetaString, OuterClientService.DcMeta.class);
        return res;
    }

    DcMeta xpipeDcMeta() {
        return JsonUtil.fromJson(JsonUtil.xpipeDcMeta, DcMeta.class);
    }
}
