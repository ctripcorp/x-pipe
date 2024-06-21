package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 16:28
 */
public class RedisOpCrdtPersistTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtPersistTransfer redisOpCrdtPersistTransfer = new RedisOpCrdtPersistTransfer();
        // "CRDT.persist" "hailusortedset" "1" "4" - > "persist " "hailusortedset"
        byte[][] args = new byte[][]{"CRDT.persist".getBytes(), "hailusortedset".getBytes(), "1".getBytes(), "4".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtPersistTransfer.transformCrdtRedisOp(RedisOpType.CRDT_PERSIST, args);
        Assert.assertEquals(RedisOpType.PERSIST, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "PERSIST");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
    }
}
