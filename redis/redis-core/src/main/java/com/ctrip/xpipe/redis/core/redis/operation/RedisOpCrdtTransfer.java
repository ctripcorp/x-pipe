package com.ctrip.xpipe.redis.core.redis.operation;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author hailu
 * @date 2024/6/3 16:10
 */
public interface RedisOpCrdtTransfer {
    Pair<RedisOpType, byte[][]> transformCrdtRedisOp(RedisOpType redisOpType, byte[][] args);

    default String bytes2Str(byte[] arg) {
        return new String(arg, Codec.defaultCharset);
    }

    default byte[] extractValue(byte[] arg) {
        int pre = -1, cur = -1;
        int count = 0;
        for (int i = 0; i < arg.length; i++) {
            if (arg[i] == ':') {
                count++;
                pre = cur;
                cur = i;
            }
            if (count == 2) {
                int length = Integer.valueOf(new String(arg, pre + 1, cur - pre - 1, Codec.defaultCharset));
                byte[] res = new byte[length];
                System.arraycopy(arg, cur + 1, res, 0, length);
                return res;
            }
        }
        throw new IllegalArgumentException("illegal crdt value" + new String(arg));
    }
}
