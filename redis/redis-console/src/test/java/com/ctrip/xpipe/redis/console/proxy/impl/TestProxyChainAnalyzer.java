package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestProxyChainAnalyzer extends AbstractProxyChainTest implements ProxyChainAnalyzer {

    private Map<DcClusterShard, ProxyChain> chains = Maps.newConcurrentMap();

    private Map<String, ProxyChain> tunnels = Maps.newConcurrentMap();

    @PostConstruct
    public void postConstruct() {
        String tunnelId1 = generateTunnelId(), tunnelId2 = generateTunnelId();
        List<TunnelInfo> infos = Lists.newArrayList(new DefaultTunnelInfo(getProxy("jq").setHostPort(new HostPort("127.0.0.1", 443)), tunnelId1)
                .setTunnelStatsResult(genTunnelSR(tunnelId1)).setSocketStatsResult(genTunnelSSR(tunnelId1)),
                new DefaultTunnelInfo(getProxy("fra").setHostPort(new HostPort("127.0.0.3", 80)), tunnelId2)
                        .setTunnelStatsResult(genTunnelSR(tunnelId2)).setSocketStatsResult(genTunnelSSR(tunnelId2)));
        ProxyChain chain = new DefaultProxyChain("fra", "cluster1", "shard1", infos);
        addProxyChain(new DcClusterShard("fra", "cluster1", "shard1"), chain);

        String tunnelId3 = generateTunnelId(), tunnelId4 = generateTunnelId();
        infos = Lists.newArrayList(new DefaultTunnelInfo(getProxy("jq").setHostPort(new HostPort("127.0.0.1", 443)), tunnelId3)
                        .setTunnelStatsResult(genTunnelSR(tunnelId3)).setSocketStatsResult(genTunnelSSR(tunnelId3)),
                new DefaultTunnelInfo(getProxy("fra").setHostPort(new HostPort("127.0.0.3", 80)), tunnelId4)
                        .setTunnelStatsResult(genTunnelSR(tunnelId4)).setSocketStatsResult(genTunnelSSR(tunnelId4)));
        chain = new DefaultProxyChain("fra", "cluster1", "shard2", infos);
        addProxyChain(new DcClusterShard("fra", "cluster1", "shard2"), chain);
    }

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId) {
        return chains.get(new DcClusterShard(backupDcId, clusterId, shardId));
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        return tunnels.get(tunnelId);
    }

    @Override
    public List<ProxyChain> getProxyChains() {
        return Lists.newArrayList(tunnels.values());
    }

    @Override
    public void addListener(Listener listener) {

    }

    @Override
    public void removeListener(Listener listener) {

    }


    public void addProxyChain(DcClusterShard dcClusterShard, ProxyChain chain) {
        chains.put(dcClusterShard, chain);
        for(TunnelInfo info : chain.getTunnels()) {
            tunnels.put(info.getTunnelId(), chain);
        }
    }

    public void addProxyChain(String tunnelId, ProxyChain chain) {
        tunnels.put(tunnelId, chain);
    }

    @Override
    public void isleader() {

    }

    @Override
    public void notLeader() {

    }
}
