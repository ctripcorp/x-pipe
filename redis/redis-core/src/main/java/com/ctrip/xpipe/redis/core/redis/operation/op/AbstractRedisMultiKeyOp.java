package com.ctrip.xpipe.redis.core.redis.operation.op;

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
public abstract class AbstractRedisMultiKeyOp extends AbstractRedisOp implements RedisMultiKeyOp {

    private byte[] rawCmdArg;

    private List<Pair<RedisKey, byte[]>> kvs;

    public AbstractRedisMultiKeyOp(byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(rawArgs);
        this.rawCmdArg = rawArgs[0];
        this.kvs = redisKvs;
    }

    public AbstractRedisMultiKeyOp(byte[][] rawArgs, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(rawArgs, gtid);
        this.rawCmdArg = rawArgs[0];
        this.kvs = redisKvs;
    }

    public AbstractRedisMultiKeyOp(byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs) {
        super(null);
        this.rawCmdArg = rawCmdArg;
        this.kvs = redisKvs;
    }

    public AbstractRedisMultiKeyOp(byte[] rawCmdArg, List<Pair<RedisKey, byte[]>> redisKvs, String gtid) {
        super(null, gtid);
        this.rawCmdArg = rawCmdArg;
        this.kvs = redisKvs;
    }

    @Override
    public List<RedisKey> getKeys() {
        return kvs.stream().map(Pair::getKey).collect(Collectors.toList());
    }

    @Override
    public Pair<RedisKey, byte[]> getKeyValue(int idx) {
        return kvs.get(idx);
    }

    @Override
    public List<Pair<RedisKey, byte[]>> getAllKeyValues() {
        return Collections.unmodifiableList(kvs);
    }

    @Override
    public byte[][] buildRawOpArgs() {
        byte[][] rawOpArgs = super.buildRawOpArgs();
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

    protected byte[] getRawCmdArg() {
        return rawCmdArg;
    }

    protected List<Pair<RedisKey, byte[]>> subKvs(Set<Integer> needKvs) {
        List<Pair<RedisKey, byte[]> > subKvs = new ArrayList<>(needKvs.size());
        IntStream.range(0, kvs.size()).forEach(idx -> {
            if (needKvs.contains(idx)) subKvs.add(kvs.get(idx));
        });

        return subKvs;
    }

    protected abstract byte[][] innerBuildRawOpArgs();

}
