package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;

import java.util.Map;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 2:37 PM
 */
public interface RedisInfoManager {

    InfoActionContext getInfoByHostPort(HostPort hostPort);

    Map<HostPort, InfoActionContext> getAllInfos();
}
