package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpGtidParser.KEY_GTID;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public abstract class AbstractRedisOpGtidWrapper extends AbstractRedisOp implements RedisOp {

    private String gtid;

    private RedisOp innerRedisOp;

    public AbstractRedisOpGtidWrapper(String gtid, RedisOp innerRedisOp) {
        this.gtid = gtid;
        this.innerRedisOp = innerRedisOp;
    }

    @Override
    public RedisOpType getOpType() {
        return innerRedisOp.getOpType();
    }

    @Override
    public String getOpGtid() {
        return gtid;
    }

    @Override
    public Long getTimestamp() {
        return innerRedisOp.getTimestamp();
    }

    @Override
    public String getGid() {
        return innerRedisOp.getGid();
    }

    @Override
    public List<String> buildRawOpArgs() {
        List<String> wholeOpArgs = new ArrayList<>();
        wholeOpArgs.add(KEY_GTID);
        wholeOpArgs.add(gtid);
        wholeOpArgs.addAll(innerRedisOp.buildRawOpArgs());
        return wholeOpArgs;
    }
}
