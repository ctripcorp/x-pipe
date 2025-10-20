package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.UnexpectedRouteUsageInfoModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyChain;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 20, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceImplTest2 {
    @InjectMocks
    private ClusterServiceImpl clusterService;

    @Mock
    private ProxyServiceImpl proxyService;

    @Mock
    private RouteServiceImpl routeService;

    @Mock
    private ClusterDao clusterDao;

    @Mock
    private MetaCache metaCache;

    private final List<String> mockDcs = Arrays.asList("fra", "oy", "jq");

    private final List<String> mockClusters = Arrays.asList("cluster1", "cluster2", "bi-cluster1", "bi-cluster2");

    private final List<String> mockShards = Arrays.asList("shard1", "shard2");

    private final RouteMeta routeMeta1 = new RouteMeta().setId(1L).setOrgId(1).setTag("meta").setSrcDc(mockDcs.get(0))
            .setDstDc(mockDcs.get(2)).setIsPublic(true)
            .setRouteInfo("PROXYTCP://1.1.1.1:80,PROXYTCP://1.1.1.9:80 PROXYTLS://1.1.1.11:443,PROXYTLS://1.1.1.12:443  PROXYTLS://1.1.1.2:443");
    private final RouteMeta routeMeta2 = new RouteMeta().setId(2L).setOrgId(0).setTag("meta").setSrcDc(mockDcs.get(0))
            .setDstDc(mockDcs.get(2)).setIsPublic(true).setRouteInfo("PROXYTCP://1.1.1.3:80 PROXYTLS://1.1.1.4:443");
    private final RouteMeta routeMeta3 = new RouteMeta().setId(3L).setOrgId(1).setTag("meta").setSrcDc(mockDcs.get(0))
            .setDstDc(mockDcs.get(1)).setIsPublic(true).setRouteInfo("PROXYTCP://1.1.1.5:80 PROXYTLS://1.1.1.6:443");
    private final RouteMeta routeMeta4 = new RouteMeta().setId(4L).setOrgId(0).setTag("meta").setSrcDc(mockDcs.get(0))
            .setDstDc(mockDcs.get(1)).setIsPublic(true).setRouteInfo("PROXYTCP://1.1.1.7:80 PROXYTLS://1.1.1.8:443");

    private final ShardMeta shardMeta = new ShardMeta().setId(mockShards.get(0));

    private final ClusterMeta clusterMeta1 = new ClusterMeta().setId(mockClusters.get(0)).setActiveDc(mockDcs.get(2))
            .setPhase(1).setOrgId(1).setClusterDesignatedRouteIds("").setType(ClusterType.ONE_WAY.name()).addShard(shardMeta);
    private final ClusterMeta clusterMeta2 = new ClusterMeta().setId(mockClusters.get(1)).setActiveDc(mockDcs.get(2))
            .setPhase(1).setOrgId(0).setClusterDesignatedRouteIds("").setType(ClusterType.ONE_WAY.name()).addShard(shardMeta);
    private final ClusterMeta clusterMeta3 = new ClusterMeta().setId(mockClusters.get(2)).setDcs("jq, oy, fra")
            .setPhase(1).setOrgId(0).setClusterDesignatedRouteIds("1,3").setType(ClusterType.BI_DIRECTION.name()).addShard(shardMeta);
    private final ClusterMeta clusterMeta4 = new ClusterMeta().setId(mockClusters.get(3)).setDcs("jq, oy, fra")
            .setPhase(1).setOrgId(1).setClusterDesignatedRouteIds("").setType(ClusterType.BI_DIRECTION.name()).addShard(shardMeta);

    @Before
    public void beforeTest() {
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
    }

    @Test
    public void testFindUnmatchedClusterRoutes() {
        String tunnelId1 =  "127.0.0.1:1880-R(127.0.0.1:1880)-L(1.1.1.1:80)->R(1.1.1.2:443)-TCP://127.0.0.3:6380";
        String tunnelId2 =  "127.0.0.1:1880-R(127.0.0.1:1880)-L(1.1.1.3:80)->R(1.1.1.4:443)-TCP://127.0.0.3:6380";

        ProxyModel proxyModel1 = new ProxyModel().setActive(true).setDcName(mockDcs.get(0)).setId(1).setUri("PROXYTCP://1.1.1.1:8080");
        ProxyModel proxyModel2 = new ProxyModel().setActive(true).setDcName(mockDcs.get(0)).setId(1).setUri("PROXYTCP://1.1.1.3:8080");

        DefaultTunnelInfo tunnelInfo1 = new com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo(proxyModel1, tunnelId1);
        List<DefaultTunnelInfo> tunnelInfos = Lists.newArrayList(tunnelInfo1);
        ProxyChain proxyChain = new DefaultProxyChain(mockDcs.get(0), mockClusters.get(0), mockShards.get(0), mockDcs.get(2), tunnelInfos);
        when(metaCache.chooseClusterMetaRoutes(mockClusters.get(0), mockDcs.get(0), Lists.newArrayList(mockDcs.get(2))))
                .thenReturn(Maps.newHashMap(mockDcs.get(2), routeMeta1));
        // test use right route
        when(proxyService.getProxyChain(mockDcs.get(0), mockClusters.get(0), mockShards.get(0), mockDcs.get(2))).thenReturn(proxyChain);
        UnexpectedRouteUsageInfoModel useWrongRouteClusterInfos = clusterService.findUnexpectedRouteUsageInfoModel();
        Assert.assertEquals(0, useWrongRouteClusterInfos.getUnexpectedRouteUsedClusterNum());

        //test use wrong route
        tunnelInfo1 = new com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo(proxyModel2, tunnelId2);
        tunnelInfos = Lists.newArrayList(tunnelInfo1);
        proxyChain = new DefaultProxyChain(mockDcs.get(0), mockClusters.get(0), mockShards.get(0), mockDcs.get(2), tunnelInfos);
        when(proxyService.getProxyChain(mockDcs.get(0), mockClusters.get(0), mockShards.get(0), mockDcs.get(2))).thenReturn(proxyChain);
        useWrongRouteClusterInfos = clusterService.findUnexpectedRouteUsageInfoModel();
        Assert.assertEquals(1, useWrongRouteClusterInfos.getUnexpectedRouteUsedClusterNum());
        String direction = String.format("%s------>%s", mockDcs.get(0), mockDcs.get(2));
        Assert.assertEquals(Integer.valueOf(1), useWrongRouteClusterInfos.getUnexpectedRouteUsageDirectionInfos().get(direction));
        UnexpectedRouteUsageInfoModel.UnexpectedRouteUsageInfo clusterDetail
                = useWrongRouteClusterInfos.getUnexpectedRouteUsageDetailInfos().get(mockClusters.get(0)).get(0);
        Assert.assertEquals(mockClusters.get(0), clusterDetail.getClusterName());
        Assert.assertEquals(1, clusterDetail.getChooseRouteId());
        Assert.assertEquals(Sets.newHashSet(2L), clusterDetail.getUsedRouteId());
    }

    @Test
    public void testFindUsedRoutesBySrcDcNameAndClusterName() {
        RouteInfoModel routeInfoModel1 = new RouteInfoModel().setId(1).setTag("meta").setSrcDcName(mockDcs.get(0))
                .setDstDcName(mockDcs.get(2))
                .setSrcProxies(Lists.newArrayList("PROXYTCP://1.1.1.1:80", "PROXYTCP://1.1.1.9:80"))
                .setOptionalProxies(Lists.newArrayList("PROXYTLS://1.1.1.11:443", "PROXYTLS://1.1.1.12:443"))
                .setDstProxies(Lists.newArrayList("PROXYTLS://1.1.1.2:443"));

        RouteInfoModel routeInfoModel2 = new RouteInfoModel().setId(2).setTag("meta").setSrcDcName(mockDcs.get(0))
                .setDstDcName(mockDcs.get(2))
                .setSrcProxies(Lists.newArrayList("PROXYTCP://1.1.1.3:80", "PROXYTCP://1.1.1.11:80"))
                .setDstProxies(Lists.newArrayList("PROXYTLS://1.1.1.4:443"));

        String tunnelId1 =  "127.0.0.1:1880-R(127.0.0.1:1880)-L(1.1.1.1:80)->R(1.1.1.2:443)-TCP://127.0.0.3:6380";
        ProxyModel proxyModel1 = new ProxyModel().setActive(true).setDcName(mockDcs.get(0)).setId(1).setUri("PROXYTCP://1.1.1.1:8080");
        ProxyModel proxyModel2 = new ProxyModel().setActive(true).setDcName(mockDcs.get(0)).setId(1).setUri("PROXYTCP://1.1.1.3:8080");

        DefaultTunnelInfo tunnelInfo1 = new com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo(proxyModel1, tunnelId1);
        List<DefaultTunnelInfo> tunnelInfos = Lists.newArrayList(tunnelInfo1);
        ProxyChain proxyChain = new DefaultProxyChain(mockDcs.get(0), mockClusters.get(0), mockShards.get(0), mockDcs.get(2), tunnelInfos);
        when(routeService.getAllActiveRouteInfoModelsByTagAndSrcDcName(RouteMeta.TAG_META, mockDcs.get(0)))
                .thenReturn(Lists.newArrayList(routeInfoModel1, routeInfoModel2));
        when(clusterDao.findClusterByClusterName(Mockito.anyString())).thenReturn(new ClusterTbl().setClusterType("one_way"));


        when(proxyService.getProxyChains(mockDcs.get(0), mockClusters.get(0)))
                .thenReturn(Maps.newHashMap(mockDcs.get(2), Lists.newArrayList(proxyChain)));
        List<RouteInfoModel> usedRoutes = clusterService.findClusterUsedRoutesBySrcDcNameAndClusterName(mockDcs.get(0), mockClusters.get(0));
        Assert.assertEquals(1, usedRoutes.size());
        Assert.assertEquals(1L, usedRoutes.get(0).getId());
    }


    private XpipeMeta mockXpipeMeta() {
        XpipeMeta meta = new XpipeMeta();

        for (String dc : mockDcs) {
            meta.addDc(mockDcMeta(dc));
        }

        return meta;
    }

    private DcMeta mockDcMeta(String dcName) {
        DcMeta dcMeta = new DcMeta();
        dcMeta.setId(dcName);

        if(dcName.equals(mockDcs.get(0))) {
            dcMeta.addRoute(routeMeta1);
            dcMeta.addRoute(routeMeta2);
            dcMeta.addRoute(routeMeta3);
            dcMeta.addRoute(routeMeta4);
        }

        dcMeta.addCluster(clusterMeta1);
        dcMeta.addCluster(clusterMeta2);
        dcMeta.addCluster(clusterMeta3);
        dcMeta.addCluster(clusterMeta4);

        return dcMeta;
    }

}
