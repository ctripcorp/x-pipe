package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseFailException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author hailu
 * @date 2024/5/28 17:23
 */
public class RdbCrdtSortedSetParser extends AbstractRdbCrdtParser<byte[]> implements RdbParser<byte[]> {
    private STATE state = STATE.READ_INIT;

    private RdbLength header;
    private int version;
    private RdbLength valueLength;
    private RdbLength eof;
    private int readValueCount;
    private byte[] member;
    private byte[] score;

    private static final Logger logger = LoggerFactory.getLogger(RdbCrdtSortedSetParser.class);

    enum STATE {
        READ_INIT,
        READ_HEAD,
        READ_LAST_VC,
        READ_MAX_DEL_VC,
        READ_LENGTH,
        READ_MEMBER,
        READ_SCORE,
        READ_END
    }

    public RdbCrdtSortedSetParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    header = null;
                    member = null;
                    score = null;
                    valueLength = null;
                    eof = null;
                    readValueCount = 0;
                    state = STATE.READ_HEAD;
                    break;
                case READ_HEAD:
                    header = parseSigned(byteBuf);
                    if (header != null) {
                        int type = (int) header.getLenLongValue() & ((1 << 8) - 1);
                        version = ((int) (header.getLenLongValue() >> 48) & ((1 << 16) - 1));
                        if (type == ORSET_TYPE) {
                            state = STATE.READ_LAST_VC;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;
                case READ_LAST_VC:
                    byte[] lastVC = parseVectorClock(byteBuf, version);
                    if (lastVC != null) {
                        if (context.getCrdtType().isTombstone()) {
                            state = STATE.READ_MAX_DEL_VC;
                        } else {
                            state = STATE.READ_LENGTH;
                        }
                    }
                    break;
                case READ_MAX_DEL_VC:
                    byte[] maxDelVC = parseVectorClock(byteBuf, version);
                    if (maxDelVC != null) {
                        state = STATE.READ_LENGTH;
                    }
                    break;
                case READ_LENGTH:
                    valueLength = parseSigned(byteBuf);
                    if (valueLength != null) {
                        if (valueLength.getLenValue() > 0) {
                            state = STATE.READ_MEMBER;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;
                case READ_MEMBER:
                    member = parseString(byteBuf);
                    if (member != null) {
                        state = STATE.READ_SCORE;
                    }
                    break;
                case READ_SCORE:
                    score = parseRc(byteBuf);
                    if (score != null) {
                        propagateCmdIfNeed(member, score);
                        readValueCount++;
                        if (readValueCount >= valueLength.getLenValue()) {
                            state = STATE.READ_END;
                        } else {
                            state = STATE.READ_MEMBER;
                        }
                    }
                    break;
                case READ_END:
                default:
            }

            if (isFinish()) {
                propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
            }
        }
        if (isFinish()) {
            ByteBuffer.allocate(4).putInt(valueLength.getLenValue()).array();
        }
        return null;
    }

    private void propagateCmdIfNeed(byte[] member, byte[] score) {
        if (null == member || null == context.getKey() || context.getCrdtType().isTombstone()) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.ZADD,
                new byte[][]{RedisOpType.ZADD.name().getBytes(), context.getKey().get(), score, member},
                context.getKey(), member));
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        member = null;
        header = null;
        valueLength = null;
        eof = null;
        readValueCount = 0;
        state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
