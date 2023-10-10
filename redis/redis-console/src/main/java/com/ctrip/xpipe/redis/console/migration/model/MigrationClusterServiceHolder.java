package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

/**
 * @author shyin
 *
 *         Dec 11, 2016
 */
public interface MigrationClusterServiceHolder {
    ClusterService getClusterService();
    ShardService getShardService();
    AzGroupClusterRepository getAzGroupClusterRepository();
    DcService getDcService();
    RedisService getRedisService();
    MigrationService getMigrationService();

}
