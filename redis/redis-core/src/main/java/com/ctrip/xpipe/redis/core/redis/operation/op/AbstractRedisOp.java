package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public abstract class AbstractRedisOp implements RedisOp {

    private GtidSet gtidSet;

    private Long timestamp;

    private String gid;

    private List<String> rawArgs;

    public AbstractRedisOp(List<String> rawArgs) {
        this(rawArgs, null, null, null);
    }

    public AbstractRedisOp(List<String> rawArgs, GtidSet gtidSet) {
        this(rawArgs, gtidSet, null, null);
    }

    public AbstractRedisOp(List<String> rawArgs, String gid, Long timestamp) {
        this(rawArgs, null, gid, timestamp);
    }

    public AbstractRedisOp(List<String> rawArgs, GtidSet gtidSet, String gid, Long timestamp) {
        this.rawArgs = rawArgs;
        this.gtidSet = gtidSet;
        this.gid = gid;
        this.timestamp = timestamp;
    }

    @Override
    public GtidSet getOpGtidSet() {
        return gtidSet;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String getGid() {
        return gid;
    }

    @Override
    public List<String> buildRawOpArgs() {
        return rawArgs;
    }

    protected void setRawArgs(List<String> args) {
        this.rawArgs = args;
    }

    @Override
    public String toString() {
        List<String> args = buildRawOpArgs();
        return getClass().getSimpleName() + ":" + String.join(" ", args);
    }
}
