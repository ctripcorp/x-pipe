package com.ctrip.xpipe.redis.core.redis.rdb;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

/**
 * @author lishanglin
 * date 2022/5/28
 */
public interface RdbParseListener {

    void onRedisOp(RedisOp redisOp);

    void onAux(String key, String value);

    void onFinish(RdbParser<?> parser);

}
