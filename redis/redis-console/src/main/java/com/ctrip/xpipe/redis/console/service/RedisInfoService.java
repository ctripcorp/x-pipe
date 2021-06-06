package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;

import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 5:27 PM
 */
public interface RedisInfoService {

    Map<HostPort, ActionContextRetMessage<Map<String, String>>> getLocalAllInfosRetMessage();


    Map<HostPort, ActionContextRetMessage<Map<String, String>>> getGlobalAllInfosRetMessage();
}
