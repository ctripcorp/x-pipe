package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author TB
 * @date 2026/3/18 14:53
 */

public class RdbHashMetadataParser extends AbstractRdbParser<Void> {
    private static final Logger logger = LoggerFactory.getLogger(RdbHashMetadataParser.class);

    private final RdbParseContext context;
    private final RdbParser<byte[]> stringParser;

    private enum State { READ_INIT, READ_MIN_EXPIRE, READ_LEN, READ_FIELD_TTL, READ_FIELD, READ_VALUE, READ_END }
    private State state = State.READ_INIT;
    private long minExpire;
    private long fieldsLeft;
    private long currentTtl;
    private byte[] currentField;
    private byte[] currentValue;

    public RdbHashMetadataParser(RdbParseContext context) {
        this.context = context;
        this.stringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Void read(ByteBuf byteBuf) {
        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_INIT:
                    minExpire = 0;
                    fieldsLeft = 0;
                    state = State.READ_MIN_EXPIRE;
                    break;

                case READ_MIN_EXPIRE:
                    Long expire = readMillSecond(byteBuf, context);
                    if (expire == null) break;
                    minExpire = expire;
                    state = State.READ_LEN;
                    break;

                case READ_LEN:
                    RdbLength len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    fieldsLeft = len.getLenValue();
                    if (fieldsLeft == 0) {
                        state = State.READ_END;
                    } else {
                        state = State.READ_FIELD_TTL;
                    }
                    break;

                case READ_FIELD_TTL:
                    RdbLength ttlLen = parseRdbLength(byteBuf);
                    if (ttlLen == null) break;
                    currentTtl = ttlLen.getLenValue();
                    state = State.READ_FIELD;
                    break;

                case READ_FIELD:
                    currentField = stringParser.read(byteBuf);
                    if (currentField == null) break;
                    stringParser.reset();
                    state = State.READ_VALUE;
                    break;

                case READ_VALUE:
                    currentValue = stringParser.read(byteBuf);
                    if (currentValue == null) break;
                    stringParser.reset();

                    fieldsLeft--;
                    if (fieldsLeft == 0) {
                        state = State.READ_END;
                    } else {
                        state = State.READ_FIELD_TTL;
                    }
                    // 处理当前字段
                    processField();
                    break;
                case READ_END:
                default:
                    break;
            }
        }
        if (isFinish()) {
            propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
        }
        return null;
    }

    private void processField() {
        RedisKey key = context.getKey();
        if (key == null) return;

        if(currentTtl>0) {
            long expireAt = currentTtl + minExpire - 1;
            // 生成 HSET 命令
            notifyRedisOp(new RedisOpSingleKey(
                    RedisOpType.HSETEX,
                    new byte[][]{
                            RedisOpType.HSETEX.name().getBytes(),
                            key.get(),
                            HASH_PXAT,
                            (expireAt+"").getBytes(),
                            HASH_FIELDS,
                            HASH_1,
                            currentField,
                            currentValue
                    },
                    key,
                    currentField, isFinish()));
        }
    }

    @Override
    public boolean isFinish() {
        return state == State.READ_END;
    }

    @Override
    public void reset() {
        super.reset();
        state = State.READ_INIT;
        currentField = null;
        currentValue = null;
        if (stringParser != null) stringParser.reset();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
