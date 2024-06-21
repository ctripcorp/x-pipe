package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hailu
 * @date 2024/6/5 16:28
 */
public class RedisOpCrdtExpireTransferTest {

    @Test
    public void transformCrdtRedisOpTest() {
        RedisOpCrdtExpireTransfer redisOpCrdtExpireTransfer = new RedisOpCrdtExpireTransfer();
        // "CRDT.EXPIRE" "hailusortedset" "1" "1718170090362" "1718170590362" "4" - > "PEXPIREAT" "hailusortedset" "1718170590362"
        byte[][] args = new byte[][]{"CRDT.EXPIRE".getBytes(), "hailusortedset".getBytes(), "1".getBytes(), "1718170090362".getBytes(), "1718170590362".getBytes(), "4".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtExpireTransfer.transformCrdtRedisOp(RedisOpType.CRDT_EXPIRE, args);
        Assert.assertEquals(RedisOpType.PEXPIREAT, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(new String(result[0]), "PEXPIREAT");
        Assert.assertEquals(new String(result[1]), "hailusortedset");
        Assert.assertEquals(new String(result[2]), "1718170590362");
    }
}
