package com.ctrip.xpipe.redis.core.redis.operation.transfer;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpCrdtTransfer;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/4 14:17
 */
public class RedisOpCrdtMSetTransfer implements RedisOpCrdtTransfer {

    private static RedisOpCrdtMSetTransfer instance = new RedisOpCrdtMSetTransfer();

    public static RedisOpCrdtMSetTransfer getInstance() {
        return instance;
    }

    @Override
    public Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args) {
        if (args.length % 3 != 0) {
            return Pair.of(RedisOpType.UNKNOWN, args);
        }
        // "CRDT.MSET" "5" "1706184154287" "hailu2002" "hailu" "2:177847;5:38" "hailu2003" "hailu" "2:177847;5:38" - > "MSET" "hailu2002" "hailu" "hailu2003" "hailu"
        byte[][] commonArgs = new byte[(args.length / 3) * 2 - 1][];
        commonArgs[0] = RedisOpType.MSET.name().getBytes();
        for (int i = 3; i < args.length; i++) {
            if (i % 3 == 0) {
                commonArgs[i / 3 * 2 - 1] = args[i];
            }
            if (i % 3 == 1) {
                commonArgs[i / 3 * 2] = args[i];
            }
        }
        return Pair.of(RedisOpType.MSET, commonArgs);
    }
}
