package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;

import java.util.List;
import java.util.Map;

public interface ProxyChainCollector extends ConsoleLeaderAware {

    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId);

    ProxyChain getProxyChain(String tunnelId);

    List<ProxyChain> getProxyChains();

    //TODO 发布调试使用，待删除（下面四个接口）
    Map<String, Map<DcClusterShardPeer, ProxyChain>> getDcProxyChainMap();

    Map<String, DcClusterShardPeer> getTunnelClusterShardMap();

    Map<DcClusterShardPeer, ProxyChain> getShardProxyChainMap();

    Map<DcClusterShardPeer, ProxyChain> getAllProxyChains();
}
