package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.console.checker.CheckerManager;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.service.RedisInfoService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 5:35 PM
 */
public class ConsoleRedisInfoService implements RedisInfoService {

    @Autowired
    private CheckerManager checkerManager;

    @Autowired
    public ConsoleConfig consoleConfig;

    @Autowired
    public ConsoleServiceManager consoleManager;

    @Override
    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getLocalAllInfosRetMessage() {
        return checkerManager.getLeaderCheckerServices()
                .stream().map(ConsoleCheckerService::getAllLocalRedisInfos)
                .reduce((acc, another)->{
                    acc.putAll(another);
                    return acc;
                }).orElseGet(HashMap::new);
    }

    @Override
    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getGlobalAllInfosRetMessage() {
        return consoleConfig.getConsoleDomains().keySet()
                .stream().map(consoleManager::getLocalRedisInfosByDc)
                .reduce((acc, another)->{
                    acc.putAll(another);
                    return acc;
                }).orElseGet(HashMap::new);
    }
}
