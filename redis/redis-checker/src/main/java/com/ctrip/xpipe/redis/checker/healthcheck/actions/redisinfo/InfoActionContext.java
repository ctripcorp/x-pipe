package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.ParsableActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 10:08 PM
 */
public interface InfoActionContext extends ParsableActionContext<Map<String, String>, String, RedisHealthCheckInstance> {

    class Result extends ActionContextRetMessage<Map<String, String>> {}

    class ResultMap extends HashMap<HostPort, ActionContextRetMessage<Map<String, String>>> {}

    @Override
    default Map<String, String> parse(String info) {
        Map<String, String> result = new HashMap<>();
        String[] lines = info.split("\r\n");
        for (String line : lines) {
            String[] keyValues = line.split(":");
            if (keyValues.length == 2) {
                result.put(keyValues[0], keyValues[1]);
            }
        }
        return result;
    }
}
