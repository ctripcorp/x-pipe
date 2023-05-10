package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Maps;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        return null;
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        return null;
    }

    @Override
    public List<ProxyChain> getProxyChains() {
        return null;
    }

    @Override
    public Map<String, Map<DcClusterShardPeer, ProxyChain>> getDcProxyChainMap() {
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



    @Override
    public void isleader() {

    }

    @Override
    public void notLeader() {

    }
}
