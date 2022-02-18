package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.gtid.GtidSet;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public interface RedisOp {

    RedisOpType getOpType();

    GtidSet getOpGtidSet();

    Long getTimestamp();

    String getGid();

    List<String> buildRawOpArgs();

}
