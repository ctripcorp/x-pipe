package com.ctrip.xpipe.redis.core.redis.rdb.parser;

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
public class RdbCrdtSetParser extends AbstractRdbCrdtParser<byte[]> implements RdbParser<byte[]> {
    private STATE state = STATE.READ_INIT;

    private RdbLength header;
    private int version;
    private RdbLength valueLength;
    private RdbLength eof;
    private int readValueCount;
    private byte[] value;

    private static final Logger logger = LoggerFactory.getLogger(RdbCrdtSetParser.class);

    enum STATE {
        READ_INIT,
        READ_HEAD,
        READ_LAST_VC,
        READ_MAX_DEL_VC,
        READ_LENGTH,
        READ_VALUE,
        READ_CURRENT_VC,
        READ_END
    }

    public RdbCrdtSetParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    header = null;
                    value = null;
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
                        if (type == ORSET_TYPE && version >= 1) {
                            state = STATE.READ_LAST_VC;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;
                case READ_LAST_VC:
                    byte[] lastVc = parseVectorClock(byteBuf, version);
                    if (lastVc != null) {
                        if (context.getCrdtType().isTombstone()) {
                            state = STATE.READ_MAX_DEL_VC;
                        } else {
                            state = STATE.READ_LENGTH;
                        }
                    }
                    break;
                case READ_MAX_DEL_VC:
                    byte[] maxDelVc = parseVectorClock(byteBuf, version);
                    if (maxDelVc != null) {
                        state = STATE.READ_LENGTH;
                    }
                    break;
                case READ_LENGTH:
                    valueLength = parseSigned(byteBuf);
                    if (valueLength != null) {
                        if (valueLength.getLenValue() > 0) {
                            state = STATE.READ_VALUE;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;
                case READ_VALUE:
                    value = parseString(byteBuf);
                    if (value != null) {
                        state = STATE.READ_CURRENT_VC;
                    }
                    break;
                case READ_CURRENT_VC:
                    byte[] currentVc = parseVectorClock(byteBuf, version);
                    if (currentVc != null) {
                        propagateCmdIfNeed(value);
                        readValueCount++;
                        if (readValueCount >= valueLength.getLenValue()) {
                            state = STATE.READ_END;
                        } else {
                            state = STATE.READ_VALUE;
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

    private void propagateCmdIfNeed(byte[] value) {
        if (null == value || null == context.getKey() || context.getCrdtType().isTombstone()) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.SADD,
                new byte[][]{RedisOpType.SADD.name().getBytes(), context.getKey().get(), value},
                context.getKey(), value));
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        value = null;
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
