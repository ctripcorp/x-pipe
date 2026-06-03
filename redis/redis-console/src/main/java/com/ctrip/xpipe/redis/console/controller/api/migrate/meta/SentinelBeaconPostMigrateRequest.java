package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Map;

public class SentinelBeaconPostMigrateRequest {

    private Map<String, HostPort> shardMasters;

    public Map<String, HostPort> getShardMasters() {
        return shardMasters;
    }

    public void setShardMasters(Map<String, HostPort> shardMasters) {
        this.shardMasters = shardMasters;
    }
}
