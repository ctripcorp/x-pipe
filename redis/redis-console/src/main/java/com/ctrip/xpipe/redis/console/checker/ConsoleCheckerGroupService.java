package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;

import java.util.Map;

public interface ConsoleCheckerGroupService {

    HostPort getCheckerLeader(long clusterDbId);

    CommandFuture<Map<HostPort, String>> getAllHealthCheckInstance(long clusterDbId, String ip, int port, boolean isCrossRegion);

    CommandFuture<Map<HostPort, HEALTH_STATE>> getAllHealthStates(long clusterDbId, String ip, int port, boolean isCrossRegion);

}
