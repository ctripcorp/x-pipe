package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/11 20:46
 */
public class RedisOpCrdtDelTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtDelTransfer instance = new RedisOpCrdtDelTransfer();

    public static RedisOpCrdtDelTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        // "CRDT.DEL_Hash" "hailu1937" "1" "1718162603123" "1:1522;2:1500" "1:1522;2:1500" - > "DEL" "hailu1937"
        // "CRDT.DEL_REG" "hailu2002" "5" "1706184168902" "2:177847;5:39" - > "DEL" "hailu2002"
        // "CRDT.DEL_SS" "hailusortedset" "1" "1718162699792" "1:1524" - > "DEL" "hailusortedset"
        // "CRDT.DEL_Rc" "test311" "1" "1718159815744" "1:1507;2:936" "1:936:1:20,2:936:1:23" - > "DEL" "test311"
        // "CRDT.DEL_Set" "hailusadd" "1" "1718162478336" "1:1520" "1:1520" - > "DEL" "hailusadd"
        byte[][] commonArgs = new byte[2][];
        commonArgs[0] = RedisOpType.DEL.name().getBytes();
        commonArgs[1] = args[1];
        return Pair.of(RedisOpType.DEL, commonArgs);
    }
}
