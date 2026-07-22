package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMultiSubKey;
import com.ctrip.xpipe.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.*;

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

    private static final Set<ByteBuffer> NON_KEY_COMMANDS = new HashSet<>();
    private static final Set<ByteBuffer> NON_KEY_COMMANDS_WITH_ARGS = new HashSet<>();

    private static final byte[] FIELDS_BYTES = "FIELDS".getBytes();
    private static final byte[] FIELDS_BYTES_LOWER = "fields".getBytes();

    static {
        // 无参数关键字
        String[] noArgCmds = {
                "NX", "nx", "XX", "xx", "GT", "gt", "LT", "lt", "CH", "ch",
                "INCR", "incr", "KEEPTTL", "keepttl", "FNX", "fnx", "FXX", "fxx", "PERSIST", "persist"
        };
        for (String cmd : noArgCmds) {
            NON_KEY_COMMANDS.add(ByteBuffer.wrap(cmd.getBytes()));
        }
        // 带一个参数的关键字
        String[] oneArgCmds = { "EX", "ex", "PX", "px", "EXAT", "exat", "PXAT", "pxat" };
        for (String cmd : oneArgCmds) {
            NON_KEY_COMMANDS_WITH_ARGS.add(ByteBuffer.wrap(cmd.getBytes()));
            NON_KEY_COMMANDS.add(ByteBuffer.wrap(cmd.getBytes())); // 也在无参数集合中，因为需要先识别
        }
    }

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

        if(redisOpType.isFields()) {
            return parseFieldsCommand(redisOpType,args);
        }
        return parseGeneralCommands(args, pair);
    }

    private RedisOpMultiSubKey parseGeneralCommands(byte[][] args, Pair<RedisOpType, byte[][]> pair) {
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

        return new RedisOpMultiSubKey(pair.getKey(), args, key, subKeys);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private boolean nonKey(byte[] args){
        return NON_KEY_COMMANDS.contains(ByteBuffer.wrap(args));
    }

    private RedisOp parseFieldsCommand(RedisOpType opType, byte[][] args) {
        int idx = keyStartIndex;
        RedisKey key = new RedisKey(args[idx++]);

        // 跳过 FIELDS 之前的所有可选标志
        while (idx < args.length) {
            ByteBuffer tokenBuf = ByteBuffer.wrap(args[idx]);
            if (NON_KEY_COMMANDS.contains(tokenBuf)) {
                idx++;
                if (NON_KEY_COMMANDS_WITH_ARGS.contains(tokenBuf) && idx < args.length) {
                    idx++; // 跳过标志的参数
                }
            } else if (Arrays.equals(args[idx], FIELDS_BYTES) || Arrays.equals(args[idx], FIELDS_BYTES_LOWER)) {
                idx++;
                break;
            } else {
                idx++;
            }
        }

        if (idx >= args.length) throw new IllegalArgumentException("Missing numfields");
        int numFields = Integer.parseInt(new String(args[idx++]));
        List<RedisKey> subKeys = new ArrayList<>(numFields);

        for (int i = 0; i < numFields; i++) {
            if (idx >= args.length) throw new IllegalArgumentException("Incomplete field list");
            subKeys.add(new RedisKey(args[idx]));
            idx += kvNum;
        }
        return new RedisOpMultiSubKey(opType, args, key, subKeys);
    }
}
