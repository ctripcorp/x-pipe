package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;

import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface RemoteCheckerManager {

    List<HEALTH_STATE> getHealthStates(String ip, int port);

    List<Map<HostPort, HealthStatusDesc>> allInstanceHealthStatus();

    List<CheckerService> getAllCheckerServices();

    Map<String,Boolean> getAllDcIsolatedCheckResult();

    Boolean getDcIsolatedCheckResult(String dcId);

    CommandFuture<Boolean> connectDc(String dc, int connectTimeoutMilli);

}
