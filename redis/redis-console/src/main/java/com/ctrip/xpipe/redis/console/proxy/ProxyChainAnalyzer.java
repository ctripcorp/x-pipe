package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.redis.console.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyChainAnalyzer;

import java.util.List;
import java.util.Map;

public interface ProxyChainAnalyzer extends CrossDcLeaderAware {

    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId);

    ProxyChain getProxyChain(String tunnelId);

    List<ProxyChain> getProxyChains();

    void addListener(Listener listener);

    void removeListener(Listener listener);

    interface Listener {
        void onChange(Map<DcClusterShard, ProxyChain> previous, Map<DcClusterShard, ProxyChain> current);
    }

}
