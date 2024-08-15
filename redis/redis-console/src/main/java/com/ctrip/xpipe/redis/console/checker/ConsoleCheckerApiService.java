package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;

public interface ConsoleCheckerApiService {

    String PATH_HEALTH_CHECK_INSTANCE = "/api/health/check/instance/{ip}/{port}";

    String PATH_CROSS_REGION_HEALTH_CHECK_INSTANCE = "/api/health/check/cross/region//instance/{ip}/{port}";

    String PATH_HEALTH_STATUS = "/api/health/{ip}/{port}";

    String PATH_CROSS_REGION_HEALTH_STATUS = "/api/health/cross/region/{ip}/{port}";

    String getHealthCheckInstance(HostPort checker, String ip, int port);

    String getCrossRegionHealthCheckInstance(HostPort checker, String ip, int port);

    HEALTH_STATE getHealthStates(HostPort checker, String ip, int port);

    HEALTH_STATE getCrossRegionHealthStates(HostPort checker, String ip, int port);

}
