package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public interface CheckerService {

    HEALTH_STATE getInstanceStatus(String ip, int port);

}
