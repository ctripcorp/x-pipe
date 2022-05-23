package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.endpoint.HostPort;

public interface SentinelSlaveInstance extends SentinelRedisInstance {

    HostPort getMaster();

}
