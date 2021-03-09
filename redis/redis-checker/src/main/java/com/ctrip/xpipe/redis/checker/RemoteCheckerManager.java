package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface RemoteCheckerManager {

    List<HEALTH_STATE> allHealthStatus(String ip, int port);

}
