package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

public class RedisOpTransactionAdapter implements RedisOp {

    private RedisOp multiOp;

    private List<RedisOp> transactionOps;

    private RedisOp execOp;

    private long estimatedSize;

    public RedisOpTransactionAdapter() {
        this.transactionOps = new LinkedList<>();
    }

    public void addMultiOp(RedisOp multiOp) {
        this.multiOp = multiOp;
    }

    private void calculateEstimatedSize() {
        long res = 0;
        res += multiOp.estimatedSize();
        for (RedisOp transactionOp : transactionOps) {
            res += transactionOp.estimatedSize();
        }
        res += execOp.estimatedSize();
        estimatedSize = res;
    }

    public void addExecOp(RedisOp execOp) {
        this.execOp = execOp;
        this.calculateEstimatedSize();
    }

    public void addTransactionOp(RedisOp transactionOp) {
        this.transactionOps.add(transactionOp);
    }

    @Override
    public RedisOpType getOpType() {
        // TODO for log, not sure
        return RedisOpType.MULTI;
    }

    /*
     * MUTLI
     * SET K V
     * SET K1 V1
     * GTID <gtid> <db> EXEC
     * */
    @Override
    public String getOpGtid() {
        return execOp.getOpGtid();
    }

    @Override
    public void clearGtid() {
        this.execOp.clearGtid();
    }

    @Override
    public Long getTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getGid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[][] buildRawOpArgs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf buildRESP() {
        throw new UnsupportedOperationException();
    }

    // for memoryThreshold
    @Override
    public long estimatedSize() {
        return estimatedSize;
    }
}
