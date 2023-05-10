package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.SerializationUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestProxyChainAnalyzer extends AbstractProxyChainTest implements ProxyChainAnalyzer {

    private Map<DcClusterShardPeer, ProxyChain> chains = Maps.newConcurrentMap();

    private Map<String, ProxyChain> tunnels = Maps.newConcurrentMap();

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
    }

    @Override
    public Map<DcClusterShardPeer, ProxyChain> getClusterShardChainMap() {
        return chains.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> SerializationUtils.clone(e.getValue())));
    }

    @Override
    public void addListener(Listener listener) {

    }

    @Override
    public void removeListener(Listener listener) {

    }


    public void addProxyChain(DcClusterShardPeer dcClusterShardPeer, ProxyChain chain) {
        chains.put(dcClusterShardPeer, chain);
        for(DefaultTunnelInfo info : chain.getTunnelInfos()) {
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
