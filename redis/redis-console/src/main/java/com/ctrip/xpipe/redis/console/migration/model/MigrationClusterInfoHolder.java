package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;

import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 *         Dec 11, 2016
 */
public interface MigrationClusterInfoHolder {
    MigrationStatus getStatus();

    MigrationClusterTbl getMigrationCluster();
    
    List<MigrationShard> getMigrationShards();
    ClusterTbl getCurrentCluster();
    String clusterName();
    Map<Long, ShardTbl> getClusterShards();
    Map<Long, DcTbl> getClusterDcs();

}
