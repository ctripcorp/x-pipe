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

        RedisKey key = new RedisKey(args[keyStartIndex]);
        int dataStart = keyStartIndex + 1;          // 主键之后的数据起始位置
        int subKeyStart;
        int subKeyCount;

        if (redisOpType.isFields()) {
            // 1. 跳过 FIELDS 之前的所有标志
            int idx = dataStart;
            while (idx < args.length) {
                if (isFieldsKeyword(args[idx])) {
                    idx++;
                    break;
                }
                int skipped = skipNonKeyTokenIfPresent(args, idx);
                if (skipped > idx) {
                    idx = skipped;
                } else {
                    idx++;
                }
            }
            if (idx >= args.length) throw new IllegalArgumentException("Missing numfields");
            subKeyCount = Integer.parseInt(new String(args[idx++]));
            subKeyStart = idx;
        } else {
            // 2. 普通命令：跳过开头的非键标志（如 ZADD 的 NX/EX 等）
            subKeyStart = skipLeadingNonKeyTokens(args, dataStart);
            subKeyCount = (args.length - subKeyStart) / kvNum;
        }

        // 3. 共用同一套子键提取逻辑
        List<RedisKey> subKeys = extractSubKeys(args, subKeyStart, subKeyCount);
        RedisOpType finalOpType = redisOpType.isFields() ? redisOpType : pair.getKey();
        return new RedisOpMultiSubKey(finalOpType, args, key, subKeys);
    }

    /**
     * 统一子键提取：从 start 开始，每次取一个子键，然后按 kvNum 跳过值部分。
     * 支持 kvReverse（反向模式）和 ZADD 非键标志跳过。
     */
    private List<RedisKey> extractSubKeys(byte[][] args, int start, int count) {
        List<RedisKey> subKeys = new ArrayList<>(count);
        int i = start;
        for (int k = 0; k < count; k++) {
            if (kvReverse) {
                // 反向顺序：值在前，子键在后
                if (kvNum == 2 && RedisOpType.ZADD == redisOpType) {
                    int skipped = skipNonKeyTokenIfPresent(args, i);
                    if (skipped > i) {
                        i = skipped;
                        k--;    // 未消耗子键，重新处理当前子键
                        continue;
                    }
                }
                int keyOffset = kvNum - 1;        // kvNum=2 → offset=1, kvNum=3 → offset=2
                subKeys.add(new RedisKey(args[i + keyOffset]));
                i += kvNum;
            } else {
                subKeys.add(new RedisKey(args[i]));
                i += kvNum;
            }
        }
        return subKeys;
    }

    /** 跳过从 index 开始的所有连续非键标志，返回第一个非标志的索引 */
    private int skipLeadingNonKeyTokens(byte[][] args, int index) {
        while (index < args.length) {
            int skipped = skipNonKeyTokenIfPresent(args, index);
            if (skipped == index) break;
            index = skipped;
        }
        return index;
    }

    /** 跳过单个非键标志（及其可能的参数），返回新的索引 */
    private int skipNonKeyTokenIfPresent(byte[][] args, int index) {
        if (index >= args.length || !nonKey(args[index])) return index;
        index++;
        if (index < args.length && NON_KEY_COMMANDS_WITH_ARGS.contains(ByteBuffer.wrap(args[index - 1]))) {
            index++;
        }
        return index;
    }

    private boolean isFieldsKeyword(byte[] arg) {
        return Arrays.equals(arg, FIELDS_BYTES) || Arrays.equals(arg, FIELDS_BYTES_LOWER);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private boolean nonKey(byte[] arg) {
        return NON_KEY_COMMANDS.contains(ByteBuffer.wrap(arg));
    }
}