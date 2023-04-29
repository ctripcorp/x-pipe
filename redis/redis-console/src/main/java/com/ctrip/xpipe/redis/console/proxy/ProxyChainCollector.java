package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;

import java.util.List;
import java.util.Map;

public interface ProxyChainCollector extends ConsoleLeaderAware {

    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId);

    ProxyChain getProxyChain(String tunnelId);

    List<ProxyChain> getProxyChains();

    Map<DcClusterShardPeer, ProxyChain> getAllProxyChains();
}
