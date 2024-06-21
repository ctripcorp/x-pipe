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
        // // "CRDT.DEL_Hash" "hailu1937" "1" "1718162603123" "1:1522;2:1500" "1:1522;2:1500" - > "DEL" "hailu1937"
        byte[][] args = new byte[][]{"CRDT.DEL_Hash".getBytes(), "hailu1937".getBytes(), "1".getBytes(), "1718162603123".getBytes(), "1:1522;2:1500".getBytes(), "1:1522;2:1500".getBytes()};
        Pair<RedisOpType, byte[][]> redisOpTypePair = redisOpCrdtDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_DEL_HASH, args);
        Assert.assertEquals(RedisOpType.DEL, redisOpTypePair.getKey());
        byte[][] result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "DEL");
        Assert.assertEquals(new String(result[1]), "hailu1937");

        // "CRDT.DEL_Set" "hailusadd" "1" "1718162478336" "1:1520" "1:1520" - > "DEL" "hailusadd" - > "DEL" "test311"
        args = new byte[][]{"CRDT.DEL_Set".getBytes(), "hailusadd".getBytes(), "1".getBytes(), "1718162478336".getBytes(), "1:1520".getBytes(), "1:1520".getBytes()};
        redisOpTypePair = redisOpCrdtDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_DEL_SET, args);
        Assert.assertEquals(RedisOpType.DEL, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "DEL");
        Assert.assertEquals(new String(result[1]), "hailusadd");

        // "CRDT.DEL_Set" "hailusadd" "1" "1718162478336" "1:1520" "1:1520" - > "DEL" "hailusadd" - > "DEL" "test311"
        args = new byte[][]{"CRDT.DEL_Set".getBytes(), "hailusadd".getBytes(), "1".getBytes(), "1718162478336".getBytes(), "1:1520".getBytes(), "1:1520".getBytes()};
        redisOpTypePair = redisOpCrdtDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_DEL_SS, args);
        Assert.assertEquals(RedisOpType.DEL, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "DEL");
        Assert.assertEquals(new String(result[1]), "hailusadd");

        // "CRDT.DEL_Rc" "test311" "1" "1718159815744" "1:1507;2:936" "1:936:1:20,2:936:1:23" - > "DEL" "test311"
        args = new byte[][]{"CRDT.DEL_Rc".getBytes(), "test311".getBytes(), "1".getBytes(), "1718159815744".getBytes(), "1:1507;2:936".getBytes(), "1:936:1:20,2:936:1:23".getBytes()};
        redisOpTypePair = redisOpCrdtDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_DEL_RC, args);
        Assert.assertEquals(RedisOpType.DEL, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "DEL");
        Assert.assertEquals(new String(result[1]), "test311");

        // "CRDT.DEL_REG" "hailu2002" "5" "1706184168902" "2:177847;5:39" - > "DEL" "hailu2002"
        args = new byte[][]{"CRDT.DEL_REG".getBytes(), "hailu2002".getBytes(), "5".getBytes(), "1706184168902".getBytes(), "2:177847;5:39".getBytes()};
        redisOpTypePair = redisOpCrdtDelTransfer.transformCrdtRedisOp(RedisOpType.CRDT_DEL_REG, args);
        Assert.assertEquals(RedisOpType.DEL, redisOpTypePair.getKey());
        result = redisOpTypePair.getValue();
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(new String(result[0]), "DEL");
        Assert.assertEquals(new String(result[1]), "hailu2002");
    }
}
