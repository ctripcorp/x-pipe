package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public interface RedisMultiKeyOp<T> extends RedisOp {

    List<RedisKey> getKeys();

    Pair<RedisKey, T> getKeyValue(int idx);

    List<Pair<RedisKey, T>> getAllKeyValues();

    RedisMultiKeyOp<T> subOp(Set<Integer> needKeys);

}
