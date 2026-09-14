package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;

import java.util.Collections;

/**
 * Keeper metadata used by Keeper health-check actions.
 */
public class DefaultKeeperInstanceInfo extends AbstractCheckInfo implements KeeperInstanceInfo {

    private final String dcId;

    private final String shardId;

    private final Long shardDbId;

    private final HostPort hostPort;

    public DefaultKeeperInstanceInfo(String dcId, String clusterId, String shardId, Long shardDbId,
                                     HostPort hostPort, String activeDc, ClusterType clusterType) {
        super(clusterId, activeDc, clusterType, Collections.emptyList());
        this.dcId = dcId;
        this.shardId = shardId;
        this.shardDbId = shardDbId;
        this.hostPort = hostPort;
    }

    @Override
    public String getShardId() {
        return shardId;
    }

    @Override
    public Long getShardDbId() {
        return shardDbId;
    }

    @Override
    public String getDcId() {
        return dcId;
    }

    @Override
    public HostPort getHostPort() {
        return hostPort;
    }

    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, activeDc:%s, %s",
                dcId, clusterId, shardId, hostPort, activeDc, clusterType);
    }
}
