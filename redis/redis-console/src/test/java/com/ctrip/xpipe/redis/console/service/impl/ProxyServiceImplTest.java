package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyChain;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jul 26, 2018
 */
public class ProxyServiceImplTest extends AbstractConsoleIntegrationTest {
    @Autowired
    private ProxyServiceImpl service;

    private ProxyModel proxy1, proxy2;
    @Mock
    private ShardService shardService;
    @Mock
    private ClusterService clusterService;
    @Mock
    private ProxyChainCollector collector;

    private static final String DC_AWS = "FRA-AWS";

    private static final String DC_RB = "SHARB";

    private static final String DC_OY = "SHAOY";

    private static final String CLUSTER_NAME = "cluster";

    private static final String SHARD_NAME = "shard";

    private ProxyChain rbChain = new DefaultProxyChain(DC_AWS, CLUSTER_NAME, SHARD_NAME, DC_RB, null);

    private ProxyChain oyChain = new DefaultProxyChain(DC_AWS, CLUSTER_NAME, SHARD_NAME, DC_RB, null);

    @Before
    public void beforeProxyServiceImplTest() {
        proxy1 = new ProxyModel().setActive(true).setDcName(dcNames[0]).setId(1).setUri("PROXYTCP://127.0.0.1:8080");
        proxy2 = new ProxyModel().setActive(false).setDcName(dcNames[0]).setId(2).setUri("PROXYTCP://127.0.0.2:8080");

        service.addProxy(proxy1);
        service.addProxy(proxy2);

        List<ShardTbl> shards = new ArrayList<ShardTbl>();
        shards.add(new ShardTbl().setShardName(SHARD_NAME));
        List<DcTbl> peerDcs = new ArrayList<DcTbl>();
        peerDcs.add(new DcTbl().setDcName(DC_AWS).setClusterName(CLUSTER_NAME));
        peerDcs.add(new DcTbl().setDcName(DC_RB).setClusterName(CLUSTER_NAME));
        peerDcs.add(new DcTbl().setDcName(DC_OY).setClusterName(CLUSTER_NAME));
        when(shardService.findAllShardNamesByClusterName(CLUSTER_NAME)).thenReturn(shards);
        when(clusterService.getClusterRelatedDcs(CLUSTER_NAME)).thenReturn(peerDcs);
        when(collector.getProxyChain(DC_AWS, CLUSTER_NAME, SHARD_NAME, DC_RB)).thenReturn(rbChain);
        when(collector.getProxyChain(DC_AWS, CLUSTER_NAME, SHARD_NAME, DC_OY)).thenReturn(oyChain);

        service.setShardService(shardService)
                .setClusterService(clusterService)
                .setProxyChainCollector(collector);
    }

    @Test
    public void testGetActiveProxies() {
        List<ProxyModel> routes = service.getAllProxies();
        Collections.sort(routes, new Comparator<ProxyModel>() {
            @Override
            public int compare(ProxyModel o1, ProxyModel o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        Assert.assertEquals(Lists.newArrayList(proxy1, proxy2), routes);
    }

    @Test
    public void testGetAllProxies() {
        List<ProxyModel> routes = service.getActiveProxies();
        Collections.sort(routes, new Comparator<ProxyModel>() {
            @Override
            public int compare(ProxyModel o1, ProxyModel o2) {
                return (int) (o1.getId() - o2.getId());
            }
        });
        Assert.assertEquals(Lists.newArrayList(proxy1), routes);
    }

    @Test
    public void testUpdateProxy() {
        String newUri = "TCP://127.0.0.1:6379";
        proxy1.setUri(newUri);
        service.updateProxy(proxy1);

        ProxyModel proxy = null;
        for (ProxyModel mode : service.getAllProxies()) {
            if (mode.getId() == proxy1.getId()) {
                proxy = mode;
                break;
            }
        }
        Assert.assertEquals(newUri, proxy.getUri());
    }

    @Test
    public void testDeleteProxy() {
        service.deleteProxy(proxy1.getId());
        Assert.assertEquals(Lists.newArrayList(proxy2), service.getAllProxies());
    }

    @Test
    public void testAddProxy() {
        ProxyModel proxy3 = new ProxyModel().setActive(false).setDcName(dcNames[0]).setId(3).setUri("PROXYTCP://127.0.0.1:8080");
        service.addProxy(proxy3);
        Assert.assertEquals(Lists.newArrayList(proxy1, proxy2, proxy3), service.getAllProxies());
    }

    @Test
    public void testGetActiveProxyTbls() {
        List<ProxyTbl> proxyTbls = service.getActiveProxyTbls();
        for (ProxyTbl proto : proxyTbls) {
            Assert.assertTrue(proto.isActive());
        }
    }

    @Test
    public void testGetProxyAll() {
        Assert.assertNotNull(service.getAllProxies());
    }

    @Test
    public void testGetProxyChains() {
        Map<String, List<ProxyChain>> chains = service.getProxyChains(DC_AWS, CLUSTER_NAME);
        Assert.assertEquals(1, chains.size());
        Assert.assertTrue(chains.containsKey(SHARD_NAME));
        Assert.assertTrue(chains.get(SHARD_NAME).contains(rbChain));
        Assert.assertTrue(chains.get(SHARD_NAME).contains(oyChain));
    }

    @Test
    public void testGetActiveProxyUrisByDc() {
        List<String> proxyUris = service.getActiveProxyUrisByDc(dcNames[0]);

        Assert.assertEquals(1, proxyUris.size());
        Assert.assertEquals(Lists.newArrayList(proxy1.getUri()), proxyUris);
    }

    @Test
    public void testProxyIdUriMap () {
        Map<Long, String> proxyUriMap = service.proxyIdUriMap();
        Assert.assertEquals(proxy1.getUri(), proxyUriMap.get(proxy1.getId()));
    }

    @Test
    public void testProxyUriIdMap () {
        Map<String, Long> proxyUriIdMap = service.proxyUriIdMap();
        Assert.assertEquals(Optional.ofNullable(proxy1.getId()), Optional.ofNullable(proxyUriIdMap.get(proxy1.getUri())));
    }

}