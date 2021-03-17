package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public class DefaultRemoteCheckerManager implements RemoteCheckerManager {

    private Map<String, CheckerService> remoteCheckers;

    private CheckerConfig config;

    private Logger logger = LoggerFactory.getLogger(DefaultRemoteCheckerManager.class);

    public DefaultRemoteCheckerManager(CheckerConfig checkerConfig) {
        this.remoteCheckers = new HashMap<>();
        this.config = checkerConfig;
    }

    @Override
    public List<HEALTH_STATE> allHealthStatus(String ip, int port) {
        Set<String> checkerAddressList = config.getAllCheckerAddress();
        List<HEALTH_STATE> result = new ArrayList<>();

        checkerAddressList.forEach(checker -> {
            try {
                if (!remoteCheckers.containsKey(checker)) remoteCheckers.put(checker, new DefaultCheckerService(checker));
                HEALTH_STATE state = remoteCheckers.get(checker).getInstanceStatus(ip, port);
                result.add(state);
            } catch (Throwable th) {
                logger.info("[allHealthStatus][{}] fail", checker, th);
            }
        });

        return result;
    }

}
