package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;

import java.util.Map;

public interface ProxyChainAnalyzer extends ConsoleLeaderAware {

    Map<DcClusterShardPeer, ProxyChain> getClusterShardChainMap();

    void addListener(Listener listener);

    void removeListener(Listener listener);

    interface Listener {
        void onChange(Map<DcClusterShardPeer, ProxyChain> previous, Map<DcClusterShardPeer, ProxyChain> current);
    }

}
