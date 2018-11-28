package com.ctrip.xpipe.redis.console.proxy;

import java.util.List;

public interface ProxyChainAnalyzer {

    ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId);

    ProxyChain getProxyChain(String tunnelId);

    List<ProxyChain> getProxyChains();

}
