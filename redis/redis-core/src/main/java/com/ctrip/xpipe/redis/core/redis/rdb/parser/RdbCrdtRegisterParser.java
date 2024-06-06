package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.exception.RdbParseFailException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hailu
 * @date 2024/5/28 17:23
 */
public class RdbCrdtRegisterParser extends AbstractRdbParser<byte[]> implements RdbParser<byte[]> {
    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private RdbLength header;
    private int version;

    private RdbLength currentType;
    private RdbLength currentValue;
    private RdbLength vcLength;

    private RdbLength gid;
    private RdbLength timestamp;
    private RdbLength valueType;
    private RdbLength eof;

    private int readVCLen;
    private byte[] val;

    private static final Logger logger = LoggerFactory.getLogger(RdbCrdtRegisterParser.class);

    enum STATE {
        READ_INIT,
        READ_HEAD,
        READ_GID,
        READ_TIMESTAMP,
        READ_VECTOR_CLOCK,
        READ_VAL,
        READ_EOF,
        READ_END
    }

    public RdbCrdtRegisterParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    header = null;
                    currentType = null;
                    currentValue = null;
                    gid = null;
                    timestamp = null;
                    val = null;
                    state = STATE.READ_HEAD;
                    vcLength = null;
                    valueType = null;
                    eof = null;
                    readVCLen = 0;
                    break;
                case READ_HEAD:
                    if (header == null) {
                        header = parseLongSigned(byteBuf);
                    }
                    if (header == null) {
                        break;
                    }
                    resetLocal();
                    int type = (int) header.getLenLongValue() & ((1 << 8) - 1);
                    version = ((int) (header.getLenLongValue() >> 48) & ((1 << 16) - 1));
                    if (type == 0) {
                        state = STATE.READ_GID;
                    } else {
                        state = STATE.READ_EOF;
                        reset();
                    }

                    break;

                case READ_GID:
                    if (gid == null) {
                        gid = parseLongSigned(byteBuf);
                    }
                    if (gid == null) {
                        break;
                    }
                    resetLocal();
                    state = STATE.READ_TIMESTAMP;
                    break;

                case READ_TIMESTAMP:
                    if (timestamp == null) {
                        timestamp = parseLongSigned(byteBuf);
                    }
                    if (timestamp == null) {
                        break;
                    }
                    resetLocal();
                    state = STATE.READ_VECTOR_CLOCK;
                    break;

                case READ_VECTOR_CLOCK:
                    if (version == 0) {
                        RdbParser<byte[]> subKeyParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
                        byte[] vcStr = subKeyParser.read(byteBuf);
                        if (null != vcStr) {
                            subKeyParser.reset();
                        }
                        state = STATE.READ_VAL;
                    } else if (version == 1) {
                        if (vcLength == null) {
                            vcLength = parseLongSigned(byteBuf);
                            if (vcLength == null) {
                                break;
                            }
                            resetLocal();
                        }
                        if (vcLength.getLenValue() == 1) {
                            RdbLength vcLock = parseLongSigned(byteBuf);
                            if (vcLock == null) {
                                break;
                            }
                            resetLocal();
                        } else {
                            for (int i = readVCLen; i < vcLength.getLenValue(); i++) {
                                RdbLength vcLock = parseLongSigned(byteBuf);
                                if (vcLock == null) {
                                    break;
                                }
                                readVCLen++;
                                resetLocal();
                            }
                            if (readVCLen != vcLength.getLenValue()) {
                                break;
                            }
                        }
                        state = STATE.READ_VAL;
                    }
                    break;

                case READ_VAL:
                    if (!context.getCrdtType().isTombstone()){
                        if (valueType == null) {
                            valueType = parseRdbLength(byteBuf);
                        }
                        if (valueType == null) {
                            break;
                        }
                        if (valueType.getLenValue() != 5) {
                            throw new RdbParseFailException("crdt rdb parse val:" + valueType.getLenValue());
                        }
                        RdbParser<byte[]> subKeyParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
                        val = subKeyParser.read(byteBuf);
                        if (val == null) {
                            break;
                        }
                        subKeyParser.reset();
                    }
                    state = STATE.READ_EOF;
                case READ_EOF:
                    if (eof == null) {
                        eof = parseRdbLength(byteBuf);
                    }
                    if (eof == null) {
                        break;
                    }
                    if (eof.getLenValue() != 0) {
                        throw new RdbParseFailException("crdt rdb parse eof" + eof.getLenValue());
                    }
                    state = STATE.READ_END;

                    break;
                case READ_END:
                default:
            }

            if (isFinish()) {
                propagateCmdIfNeed();
            }
        }

        return val;
    }


    private RdbLength parseLongSigned(ByteBuf byteBuf) {
        if (currentType == null) {
            currentType = parseRdbLength(byteBuf);
        }
        if (currentType != null) {
            if (currentType.getLenValue() != 2) {
                throw new XpipeRuntimeException("crdt opcode error" + currentType.getLenType());
            }
            if (currentValue == null) {
                currentValue = parseRdbLength(byteBuf);
            }
            if (currentValue != null) {
                return currentValue;
            }
        }
        return null;
    }

    private void propagateCmdIfNeed() {
        if (null == val
                || null == context.getKey()
                || !RdbParseContext.RdbType.CRDT.equals(context.getCurrentType())) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.SET,
                new byte[][]{RedisOpType.SET.name().getBytes(), context.getKey().get(), val},
                context.getKey(), val));
        propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
    }

    private void resetLocal() {
        currentType = null;
        currentValue = null;
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        this.state = STATE.READ_INIT;
        val = null;
        header = null;
        currentType = null;
        currentValue = null;
        gid = null;
        timestamp = null;
        vcLength = null;
        valueType = null;
        eof = null;
        readVCLen = 0;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
