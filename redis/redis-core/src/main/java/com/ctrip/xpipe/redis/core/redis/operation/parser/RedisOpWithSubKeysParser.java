package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMultiKVs;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMultiSubKey;
import com.ctrip.xpipe.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author tb
 * <p>
 * 2025/10/10 15:27
 */
public class RedisOpWithSubKeysParser extends AbstractRedisOpParser implements RedisOpParser {

    private RedisOpType redisOpType;
    private Integer keyStartIndex;
    private Integer kvNum;
    private boolean kvReverse;

    private byte[] NX_BYTES = new byte[]{'N','X'};
    private byte[] XX_BYTES = new byte[]{'X','X'};
    private byte[] GT_BYTES = new byte[]{'G','T'};
    private byte[] LT_BYTES = new byte[]{'L','T'};
    private byte[] CH_BYTES = new byte[]{'C','H'};
    private byte[] INCR_BYTES = new byte[]{'I','N','C','R'};

    public RedisOpWithSubKeysParser(RedisOpType redisOpType, int keyStartIndex, int kvNum,boolean kvReverse) {
        this.redisOpType = redisOpType;
        this.keyStartIndex = keyStartIndex;
        this.kvNum = kvNum;
        this.kvReverse = kvReverse;
    }

    @Override
    public RedisOp parse(byte[][] args) {
        Pair<RedisOpType, byte[][]> pair = redisOpType.transfer(redisOpType, args);
        args = pair.getValue();

        // 计算容量 - 主键 + 子键数量
        int subKeyCount = (args.length - keyStartIndex-1) / kvNum;
        int capacity = 1 + subKeyCount;
        List<RedisKey> subKeys = new ArrayList<>(capacity);

        int i = keyStartIndex;
        RedisKey key = new RedisKey(args[i++]);
        // 处理子键
        while (i < args.length) {
            RedisKey subKey = null;

            if (!kvReverse) {
                // 正常顺序：子键在前
                subKey = new RedisKey(args[i]);
                i += kvNum; // 跳过值部分
            } else {
                // 反向顺序：值在前，子键在后
                if (kvNum == 2) {
                    if(RedisOpType.ZADD == redisOpType && nonKey(args[i])){
                        i++;
                        continue;
                    }
                    subKey = new RedisKey(args[i + 1]); // 跳过第一个值，取第二个作为子键
                    i += 2;
                } else if (kvNum == 3) {
                    subKey = new RedisKey(args[i + 2]); // 跳过前两个值，取第三个作为子键
                    i += 3;
                }
            }

            subKeys.add(subKey);
        }

        return new RedisOpMultiSubKey(pair.getKey(), args, key,subKeys);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private boolean nonKey(byte[] args){
        return Arrays.equals(NX_BYTES,args) || Arrays.equals(XX_BYTES,args)
                || Arrays.equals(GT_BYTES,args) || Arrays.equals(LT_BYTES,args)
                || Arrays.equals(CH_BYTES,args) || Arrays.equals(INCR_BYTES,args);
    }
}
