package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/12
 */
@Component
public class DefaultRemoteCheckerManager implements RemoteCheckerManager {

    @Override
    public List<HEALTH_STATE> allHealthStatus(String ip, int port) {
        return null;
    }

}
