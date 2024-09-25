package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.endpoint.ClusterShardHostPort;

public interface OuterClientAggregator {

    void markInstance(ClusterShardHostPort info);

}
