package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestProxyChainCollector extends AbstractProxyChainTest implements ProxyChainCollector {
    private ProxyModel proxyModelJq;
    private ProxyModel proxyModelOy;

    private Map<DcClusterShardPeer, ProxyChain> shardProxyChainMap = Maps.newConcurrentMap();

    private volatile Map<String, DcClusterShardPeer> tunnelClusterShardMap = Maps.newConcurrentMap();

    private Map<String, Map<DcClusterShardPeer, ProxyChain>> dcProxyChainMap = Maps.newConcurrentMap();

    @PostConstruct
    public void postConstruct() {
        String tunnelId1 = generateTunnelId(), tunnelId2 = generateTunnelId();
        List<DefaultTunnelInfo> infos = Lists.newArrayList(new com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo(getProxy("jq").setHostPort(new HostPort("127.0.0.1", 443)), tunnelId1)
                        .setTunnelStatsResult(genTunnelSR(tunnelId1)).setTunnelSocketStatsResult(genTunnelSSR(tunnelId1)),
                new com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo(getProxy("fra").setHostPort(new HostPort("127.0.0.3", 80)), tunnelId2)
                        .setTunnelStatsResult(genTunnelSR(tunnelId2)).setTunnelSocketStatsResult(genTunnelSSR(tunnelId2)));
        ProxyChain chain = new DefaultProxyChain("fra", "cluster1", "shard1", "sharb", infos);
        addProxyChain(new DcClusterShardPeer("fra", "cluster1", "shard1","sharb"), chain);

        String tunnelId3 = generateTunnelId(), tunnelId4 = generateTunnelId();
        infos = Lists.newArrayList(new com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo(getProxy("jq").setHostPort(new HostPort("127.0.0.1", 443)), tunnelId3)
                        .setTunnelStatsResult(genTunnelSR(tunnelId3)).setTunnelSocketStatsResult(genTunnelSSR(tunnelId3)),
                new com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo(getProxy("fra").setHostPort(new HostPort("127.0.0.3", 80)), tunnelId4)
                        .setTunnelStatsResult(genTunnelSR(tunnelId4)).setTunnelSocketStatsResult(genTunnelSSR(tunnelId4)));
        chain = new DefaultProxyChain("fra", "cluster1", "shard2","sharb", infos);
        addProxyChain(new DcClusterShardPeer("fra", "cluster1", "shard2","sharb"), chain);

        proxyModelJq = new ProxyModel();
        proxyModelJq.setActive(true).setUri("PROXYTCP://10.15.190.149:80").setId(13)
                .setMonitorActive(true).setDcName("SIN-AWS").setHostPort(new HostPort("10.15.190.149", 80));
        List<DefaultTunnelInfo> tunnelInfos1 = new ArrayList<>();
        tunnelInfos1.add(new DefaultTunnelInfo(proxyModelJq,
                "10.15.130.218:51564-R(10.15.130.218:51564)-L(10.15.190.149:40062)->R(117.86.34.9:443)-TCP://10.130.253.185:6381"));
        DefaultProxyChain proxyChainJq = new DefaultProxyChain("SIN-AWS", "test_fws", "test_fws_1", "NTGXH", tunnelInfos1);
        DcClusterShardPeer dcClusterShardPeer1 = new DcClusterShardPeer("SIN-AWS", "test_fws", "test_fws_1", "NTGXH");
        Map<DcClusterShardPeer, ProxyChain> map1 = new HashMap<>();
        map1.put(dcClusterShardPeer1, proxyChainJq);
        dcProxyChainMap.put("SIN-AWS", map1);

        proxyModelOy = new ProxyModel();
        proxyModelOy.setActive(true).setUri("PROXYTCP://10.128.12.151:80").setId(17)
                .setMonitorActive(true).setDcName("NTGXH").setHostPort(new HostPort("10.128.12.151", 80));
        List<DefaultTunnelInfo> tunnelInfos2 = new ArrayList<>();
        tunnelInfos2.add(new DefaultTunnelInfo(proxyModelOy,
                "10.15.130.218:51564-R(10.4.104.154:7760)-L(10.128.12.151:57714)->R(10.130.253.185:6381)-TCP://10.130.253.185:6381"));
        DefaultProxyChain proxyChainOy = new DefaultProxyChain("SIN-AWS", "test_fws", "test_fws_1", "NTGXH", tunnelInfos2);
        DcClusterShardPeer dcClusterShardPeer2 = new DcClusterShardPeer("SIN-AWS", "test_fws", "test_fws_1", "NTGXH");
        Map<DcClusterShardPeer, ProxyChain> map2 = new HashMap<>();
        map2.put(dcClusterShardPeer2, proxyChainOy);
        dcProxyChainMap.put("UAT", map2);
    }

     void updateShardProxyChainMap() {
        Map<DcClusterShardPeer, ProxyChain> tempShardProxyChain = Maps.newConcurrentMap();
        Map<String, DcClusterShardPeer> tempTunnelClusterShardMap = Maps.newConcurrentMap();
        logger.info("update proxy chains {}", dcProxyChainMap);
        dcProxyChainMap.forEach((dc, proxyChainMap) -> {
            proxyChainMap.forEach((clusterShard, proxyChain) -> {
                if (tempShardProxyChain.containsKey(clusterShard)) {
                    tempShardProxyChain.get(clusterShard).getTunnelInfos().add(proxyChain.getTunnelInfos().get(0));
                } else {
                    tempShardProxyChain.put(clusterShard, proxyChain);
                }
                tempTunnelClusterShardMap.put(proxyChain.getTunnelInfos().get(0).getTunnelId(), clusterShard);
            });
        });
        synchronized (TestProxyChainCollector.this) {
            tunnelClusterShardMap = tempTunnelClusterShardMap;
            shardProxyChainMap = tempShardProxyChain;
        }
    }


    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId) {
        return shardProxyChainMap.get(new DcClusterShardPeer(backupDcId, clusterId, shardId, peerDcId));
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        return null;
    }

    @Override
    public List<ProxyChain> getProxyChains() {
        return shardProxyChainMap.values().stream().collect(Collectors.toList());
    }

    @Override
    public Map<String, Map<DcClusterShardPeer, ProxyChain>> getDcProxyChainMap() {
        return null;
    }

    @Override
    public Map<String, DcClusterShardPeer> getTunnelClusterShardMap() {
        return null;
    }

    @Override
    public Map<DcClusterShardPeer, ProxyChain> getShardProxyChainMap() {
        return null;
    }

    @Override
    public Map<DcClusterShardPeer, ProxyChain> getAllProxyChains() {
        return null;
    }

    public void addProxyChain(DcClusterShardPeer dcClusterShardPeer, ProxyChain chain) {
        shardProxyChainMap.put(dcClusterShardPeer, chain);
    }

    @Override
    public void isleader() {

    }

    @Override
    public void notLeader() {

    }
}
