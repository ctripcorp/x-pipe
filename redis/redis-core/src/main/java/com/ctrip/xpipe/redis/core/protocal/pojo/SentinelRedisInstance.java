package com.ctrip.xpipe.redis.core.protocal.pojo;

import java.util.Set;

public interface SentinelRedisInstance extends Instance {

    /*common things*/
    String name();

    Set<SentinelFlag> flags();

}
