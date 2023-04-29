package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
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
public class TestProxyChainCollector extends AbstractProxyChainTest implements ProxyChainCollector {

    private Map<DcClusterShardPeer, ProxyChain> chains = Maps.newConcurrentMap();

    private Map<String, ProxyChain> tunnels = Maps.newConcurrentMap();

    @PostConstruct
    public void postConstruct() {
        String tunnelId1 = generateTunnelId(), tunnelId2 = generateTunnelId();
        List<TunnelInfo> infos = Lists.newArrayList(new DefaultTunnelInfo(getProxy("jq").setHostPort(new HostPort("127.0.0.1", 443)), tunnelId1)
                .setTunnelStatsResult(genTunnelSR(tunnelId1)).setSocketStatsResult(genTunnelSSR(tunnelId1)),
                new DefaultTunnelInfo(getProxy("fra").setHostPort(new HostPort("127.0.0.3", 80)), tunnelId2)
                        .setTunnelStatsResult(genTunnelSR(tunnelId2)).setSocketStatsResult(genTunnelSSR(tunnelId2)));
        ProxyChain chain = new DefaultProxyChain("fra", "cluster1", "shard1", "sharb", infos);
        addProxyChain(new DcClusterShardPeer("fra", "cluster1", "shard1","sharb"), chain);

        String tunnelId3 = generateTunnelId(), tunnelId4 = generateTunnelId();
        infos = Lists.newArrayList(new DefaultTunnelInfo(getProxy("jq").setHostPort(new HostPort("127.0.0.1", 443)), tunnelId3)
                        .setTunnelStatsResult(genTunnelSR(tunnelId3)).setSocketStatsResult(genTunnelSSR(tunnelId3)),
                new DefaultTunnelInfo(getProxy("fra").setHostPort(new HostPort("127.0.0.3", 80)), tunnelId4)
                        .setTunnelStatsResult(genTunnelSR(tunnelId4)).setSocketStatsResult(genTunnelSSR(tunnelId4)));
        chain = new DefaultProxyChain("fra", "cluster1", "shard2","sharb", infos);
        addProxyChain(new DcClusterShardPeer("fra", "cluster1", "shard2","sharb"), chain);
    }

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId) {
        return chains.get(new DcClusterShardPeer(backupDcId, clusterId, shardId,peerDcId));
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
    public Map<DcClusterShardPeer, ProxyChain> getAllProxyChains() {
        return null;
    }


    public void addProxyChain(DcClusterShardPeer dcClusterShardPeer, ProxyChain chain) {
        chains.put(dcClusterShardPeer, chain);
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
