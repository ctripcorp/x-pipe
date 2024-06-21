package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 20:25
 */
public class RedisOpCrdtExecTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtExecTransfer redisOpCrdtExecTransfer = new RedisOpCrdtExecTransfer();
        // "CRDT.EXEC" "5"  - > "EXEC"
        byte[][] args = new byte[][]{"CRDT.EXEC".getBytes(), "5".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtExecTransfer.transformCrdtRedisOp(RedisOpType.CRDT_EXEC, args);
        Assert.assertEquals(RedisOpType.EXEC, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 1);
        Assert.assertEquals(new String(result[0]), "EXEC");
    }
}
