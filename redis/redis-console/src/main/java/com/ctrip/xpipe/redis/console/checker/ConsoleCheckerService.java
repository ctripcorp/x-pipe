package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;

import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 04, 2021 3:18 PM
 */
public interface ConsoleCheckerService {

    Map<HostPort, ActionContextRetMessage<Map<String, String>>> getAllLocalRedisInfos();
}
