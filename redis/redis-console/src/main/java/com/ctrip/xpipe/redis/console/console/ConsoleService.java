package com.ctrip.xpipe.redis.console.console;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public interface ConsoleService extends CheckerService {

    Boolean getInstancePingStatus(String ip, int port);

    Long getInstanceDelayStatus(String ip, int port);

    Long getShardDelay(long shardId);

    Long getInstanceDelayStatusFromParallelService(String ip, int port);

    Map<HostPort, Long> getAllInstanceDelayStatus();

    UnhealthyInfoModel getActiveClusterUnhealthyInstance();

    UnhealthyInfoModel getAllUnhealthyInstance();

    Map<String, Pair<HostPort, Long>> getCrossMasterDelay(String clusterId, String shardId);

    Map<String, Pair<HostPort, Long>> getCrossMasterDelayFromParallelService(String sourceDcId, String clusterId, String shardId);

    Map<HostPort, ActionContextRetMessage<Map<String, String>>> getAllLocalRedisInfos();
}
