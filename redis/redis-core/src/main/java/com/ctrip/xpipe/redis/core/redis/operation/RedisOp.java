package com.ctrip.xpipe.redis.core.redis.operation;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public interface RedisOp {

    RedisOpType getOpType();

    String getOpGtid();

    Long getTimestamp();

    String getGid();

    List<String> buildRawOpArgs();

}
