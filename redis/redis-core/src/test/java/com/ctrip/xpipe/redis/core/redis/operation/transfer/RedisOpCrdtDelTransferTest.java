package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 16:28
 */
public class RedisOpCrdtDelTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtDelTransfer redisOpCrdtDelTransfer = new RedisOpCrdtDelTransfer();
        // "CRDT.DEL_REG" "hailu2002" "5" "1706184168902" "2:177847;5:39" - > "DEL" "hailu2002"
        byte[][] args = new byte[][]{"CRDT.DEL_REG".getBytes(), "hailu2002".getBytes(), "5".getBytes(), "1706184168902".getBytes(), "2:177847;5:39".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_DEL_REG, args);
        Assert.assertEquals(RedisOpType.DEL, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "DEL");
        Assert.assertEquals(new String(result[1]), "hailu2002");
    }
}
