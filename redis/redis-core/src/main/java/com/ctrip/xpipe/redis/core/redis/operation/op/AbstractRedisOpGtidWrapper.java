package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpGtidParser.KEY_GTID;

/**
 * @author lishanglin
 * date 2022/2/18
 */
public abstract class AbstractRedisOpGtidWrapper implements RedisOp {

    private GtidSet gtidSet;

    private RedisOp innerRedisOp;

    public AbstractRedisOpGtidWrapper(GtidSet gtidSet, RedisOp innerRedisOp) {
        this.gtidSet = gtidSet;
        this.innerRedisOp = innerRedisOp;
    }

    @Override
    public RedisOpType getOpType() {
        return innerRedisOp.getOpType();
    }

    @Override
    public GtidSet getOpGtidSet() {
        return gtidSet;
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
        wholeOpArgs.add(gtidSet.toString());
        wholeOpArgs.addAll(innerRedisOp.buildRawOpArgs());
        return wholeOpArgs;
    }
}
