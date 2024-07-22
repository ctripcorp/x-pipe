package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import java.util.List;

public interface ConsoleDcCheckerService {

    List<ShardCheckerHealthCheckModel> getShardAllCheckerGroupHealthCheck(String dcId, String clusterId, String shardId);

    List<ShardCheckerHealthCheckModel> getLocalDcShardAllCheckerGroupHealthCheck(String dcId, String clusterId, String shardId);

}
