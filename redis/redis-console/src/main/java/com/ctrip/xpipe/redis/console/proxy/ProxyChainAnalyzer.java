package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;

import java.util.List;
import java.util.Map;

public interface ProxyChainAnalyzer extends LeaderAware {

    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId);

    ProxyChain getProxyChain(String tunnelId);

    List<ProxyChain> getProxyChains();

    void addListener(Listener listener);

    void removeListener(Listener listener);

    interface Listener {
        void onChange(Map<DcClusterShardPeer, ProxyChain> previous, Map<DcClusterShardPeer, ProxyChain> current);
    }

}
