package com.ctrip.xpipe.redis.core.redis.operation;

import java.util.List;

/**
 * @author TB
 * <p>
 * 2025/10/10 17:04
 */
public interface RedisMultiSubKeyOp extends RedisOp{
    RedisKey getKey();

    List<RedisKey> getAllSubKeys();
}
