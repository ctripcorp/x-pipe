package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/11 20:27
 */
public class RedisOpCrdtCounterTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtCounterTransfer redisOpCrdtCounterTransfer = new RedisOpCrdtCounterTransfer();
        // "CRDT.COUNTER" "hailu1945" "5" "1706183220461" "5:16" "4" "4:5" "6" ->set hailu1945 6
        byte[][] args = new byte[][]{"CRDT.COUNTER".getBytes(), "hailu1945".getBytes(), "5".getBytes(), "1706183220461".getBytes(), "5:16".getBytes(), "4".getBytes(), "4:5".getBytes(), "6".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtCounterTransfer.transformCrdtRedisOp(RedisOpType.CRDT_COUNTER, args);
        Assert.assertEquals(RedisOpType.SET, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "SET");
        Assert.assertEquals(new String(result[1]), "hailu1945");
        Assert.assertEquals(new String(result[2]), "6");
    }
}
