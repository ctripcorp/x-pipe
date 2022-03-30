package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public abstract class AbstractRedisOp implements RedisOp {

    private String gtid;

    private Long timestamp;

    private String gid;

    private List<String> rawArgs;

    public AbstractRedisOp(List<String> rawArgs) {
        this(rawArgs, null, null, null);
    }

    public AbstractRedisOp(List<String> rawArgs, String gtid) {
        this(rawArgs, gtid, null, null);
    }

    public AbstractRedisOp(List<String> rawArgs, String gid, Long timestamp) {
        this(rawArgs, null, gid, timestamp);
    }

    public AbstractRedisOp(List<String> rawArgs, String gtid, String gid, Long timestamp) {
        this.rawArgs = rawArgs;
        this.gtid = gtid;
        this.gid = gid;
        this.timestamp = timestamp;
    }

    @Override
    public String getOpGtid() {
        return gtid;
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
