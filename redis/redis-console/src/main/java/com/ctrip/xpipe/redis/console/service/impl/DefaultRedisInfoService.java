package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RedisInfoManager;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.service.RedisInfoService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 5:30 PM
 */
public class DefaultRedisInfoService implements RedisInfoService {

    @Autowired
    public RedisInfoManager infoManager;

    @Autowired
    public ConsoleConfig consoleConfig;

    @Autowired
    public ConsoleServiceManager consoleManager;

    @Override
    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getLocalAllInfosRetMessage(String section) {
        return InfoActionContext.toRetMessage(infoManager.getAllInfos(), section);
    }

    @Override
    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getGlobalAllInfosRetMessage(String section) {
        return consoleManager.getAllLocalRedisInfos(consoleConfig.getConsoleDomains().keySet(), section);
    }
}
