package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;

import java.util.List;

public interface ProxyChainAnalyzer extends CrossDcLeaderAware {

    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId);

    ProxyChain getProxyChain(String tunnelId);

    List<ProxyChain> getProxyChains();

}
