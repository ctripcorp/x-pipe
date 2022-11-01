package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface RedisDelayManager {

    Map<HostPort, Long> getAllDelays();

    Map<Long,Long> getAllUpstreamShardsDelays();

}
