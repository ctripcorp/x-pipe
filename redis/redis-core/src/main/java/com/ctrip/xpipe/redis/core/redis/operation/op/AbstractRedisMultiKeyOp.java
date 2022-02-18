package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public abstract class AbstractRedisMultiKeyOp<T> extends AbstractRedisOp implements RedisMultiKeyOp<T> {

    private List<Pair<RedisKey, T>> kvs;

    public AbstractRedisMultiKeyOp(List<String> rawArgs, List<Pair<RedisKey, T>> redisKvs) {
        super(rawArgs);
        this.kvs = redisKvs;
    }

    public AbstractRedisMultiKeyOp(List<String> rawArgs, List<Pair<RedisKey, T>> redisKvs, GtidSet gtidSet) {
        super(rawArgs, gtidSet);
        this.kvs = redisKvs;
    }

    public AbstractRedisMultiKeyOp(List<String> rawArgs, List<Pair<RedisKey, T>> redisKvs, String gid, Long timestamp) {
        super(rawArgs, gid, timestamp);
        this.kvs = redisKvs;
    }

    public AbstractRedisMultiKeyOp(List<String> rawArgs, List<Pair<RedisKey, T>> redisKvs, GtidSet gtidSet, String gid, Long timestamp) {
        super(rawArgs, gtidSet, gid, timestamp);
        this.kvs = redisKvs;
    }

    @Override
    public List<RedisKey> getKeys() {
        return kvs.stream().map(Pair::getKey).collect(Collectors.toList());
    }

    @Override
    public Pair<RedisKey, T> getKeyValue(int idx) {
        return kvs.get(idx);
    }

    @Override
    public List<Pair<RedisKey, T>> getAllKeyValues() {
        return Collections.unmodifiableList(kvs);
    }

    @Override
    public List<String> buildRawOpArgs() {
        List<String> rawOpArgs = super.buildRawOpArgs();
        if (null != rawOpArgs) return rawOpArgs;
        synchronized (this) {
            rawOpArgs = super.buildRawOpArgs();
            if (null == rawOpArgs) {
                rawOpArgs = innerBuildRawOpArgs();
                setRawArgs(rawOpArgs);
            }
        }

        return rawOpArgs;
    }

    protected List<Pair<RedisKey, T>> subKvs(Set<Integer> needKvs) {
        List<Pair<RedisKey, T> > subKvs = new ArrayList<>(needKvs.size());
        IntStream.range(0, kvs.size()).forEach(idx -> {
            if (needKvs.contains(idx)) subKvs.add(kvs.get(idx));
        });

        return subKvs;
    }

    protected abstract List<String> innerBuildRawOpArgs();

}
