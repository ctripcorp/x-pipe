package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface RedisDelayManager extends DelayActionListener {

    Map<HostPort, Long> getAllDelays();

}
