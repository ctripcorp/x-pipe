package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
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

    private GroupCheckerLeaderElector checkerLeaderElector;

    private Logger logger = LoggerFactory.getLogger(DefaultRemoteCheckerManager.class);

    public DefaultRemoteCheckerManager(CheckerConfig checkerConfig, GroupCheckerLeaderElector checkerLeaderElector) {
        this.remoteCheckers = new HashMap<>();
        this.config = checkerConfig;
        this.checkerLeaderElector = checkerLeaderElector;
    }

    @Override
    public List<HEALTH_STATE> getHealthStates(String ip, int port) {
        Set<String> checkerAddressList = getAllCheckerAddress();
        List<HEALTH_STATE> result = new ArrayList<>();

        checkerAddressList.forEach(checker -> {
            try {
                if (!remoteCheckers.containsKey(checker)) remoteCheckers.put(checker, new DefaultCheckerService(checker));
                HEALTH_STATE state = remoteCheckers.get(checker).getInstanceStatus(ip, port);
                result.add(state);
            } catch (Throwable th) {
                logger.info("[getHealthStates][{}] fail", checker, th);
            }
        });

        return result;
    }

    @Override
    public List<Map<HostPort, HealthStatusDesc>> allInstanceHealthStatus() {
        Set<String> checkerAddressList = getAllCheckerAddress();
        List<Map<HostPort, HealthStatusDesc>> result = new ArrayList<>();

        checkerAddressList.forEach(checker -> {
            try {
                if (!remoteCheckers.containsKey(checker)) remoteCheckers.put(checker, new DefaultCheckerService(checker));
                Map<HostPort, HealthStatusDesc> allInstanceHealthStatus = remoteCheckers.get(checker).getAllInstanceHealthStatus();
                result.add(allInstanceHealthStatus);
            } catch (Throwable th) {
                logger.info("[allInstanceHealthStatus][{}] fail", checker, th);
            }
        });

        return result;
    }

    @Override
    public List<CheckerService> getAllCheckerServices() {
        Set<String> checkerAddressList = getAllCheckerAddress();
        List<CheckerService> checkerServices = new ArrayList<>();

        checkerAddressList.forEach(checkerAddress -> {
            if (!remoteCheckers.containsKey(checkerAddress)) remoteCheckers.put(checkerAddress, new DefaultCheckerService(checkerAddress));
            checkerServices.add(remoteCheckers.get(checkerAddress));
        });

        return checkerServices;
    }

    private Set<String> getAllCheckerAddress() {
        Set<String> result = new HashSet<>();
        List<String> servers = checkerLeaderElector.getAllServers();
        String port = System.getProperty("server.port", "8080");
        if(servers.size() == 0) {
            servers.add("127.0.0.1");
        }
        for(String server : servers) {
            result.add(server + ":" + port);
        }
        return result;
    }
}
