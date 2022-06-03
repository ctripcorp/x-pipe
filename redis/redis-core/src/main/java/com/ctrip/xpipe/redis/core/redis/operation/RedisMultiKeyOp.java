package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public interface RedisMultiKeyOp extends RedisOp {

    List<RedisKey> getKeys();

    Pair<RedisKey, byte[]> getKeyValue(int idx);

    List<Pair<RedisKey, byte[]>> getAllKeyValues();

    RedisMultiKeyOp subOp(Set<Integer> needKeys);

}
