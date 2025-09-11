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
public class RdbCrdtHashParser extends AbstractRdbCrdtParser<byte[]> implements RdbParser<byte[]> {
    private static final int HASH_MAX_DEL = 1;
    private STATE state = STATE.READ_INIT;

    private RdbLength header;
    private int version;
    private RdbLength valueLength;
    private RdbLength maxDel;
    private RdbLength gid;
    private RdbLength timestamp;
    private RdbLength eof;
    private int readValueCount;
    private byte[] field;
    private byte[] value;

    private static final Logger logger = LoggerFactory.getLogger(RdbCrdtHashParser.class);

    enum STATE {
        READ_INIT,
        READ_HEAD,
        READ_TOMBSTONE_MAX_DEL,
        READ_TOMBSTONE_GID,
        READ_TOMBSTONE_TIMESTAMP,
        READ_TOMBSTONE_MAX_DEL_VC,
        READ_MAX_DEL_VC,
        READ_LAST_VC,
        READ_LENGTH,
        READ_FIELD,
        READ_VALUE,
        READ_END
    }

    public RdbCrdtHashParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    header = null;
                    field = null;
                    value = null;
                    valueLength = null;
                    eof = null;
                    timestamp = null;
                    gid = null;
                    maxDel = null;
                    readValueCount = 0;
                    state = STATE.READ_HEAD;
                    break;
                case READ_HEAD:
                    header = parseSigned(byteBuf);
                    if (header != null) {
                        int type = (int) header.getLenLongValue() & ((1 << 8) - 1);
                        version = ((int) (header.getLenLongValue() >> 48) & ((1 << 16) - 1));
                        if (type == LWW_TYPE) {
                            if (context.getCrdtType().isTombstone()) {
                                state = STATE.READ_TOMBSTONE_MAX_DEL;
                            } else {
                                state = STATE.READ_LAST_VC;
                            }
                        } else {
                            state = STATE.READ_END;
                            reset();
                        }
                    }
                    break;

                case READ_TOMBSTONE_MAX_DEL:
                    maxDel = parseSigned(byteBuf);
                    if (maxDel != null) {
                        if (maxDel.getLenValue() == HASH_MAX_DEL) {
                            state = STATE.READ_TOMBSTONE_GID;
                        } else {
                            state = STATE.READ_LAST_VC;
                        }
                    }
                    break;

                case READ_TOMBSTONE_GID:
                    gid = parseSigned(byteBuf);
                    if (gid != null) {
                        state = STATE.READ_TOMBSTONE_TIMESTAMP;
                    }
                    break;

                case READ_TOMBSTONE_TIMESTAMP:
                    timestamp = parseSigned(byteBuf);
                    if (timestamp != null) {
                        state = STATE.READ_TOMBSTONE_MAX_DEL_VC;
                    }
                    break;

                case READ_TOMBSTONE_MAX_DEL_VC:
                    byte[] maxDelVC = parseVectorClock(byteBuf, version);
                    if (maxDelVC != null) {
                        state = STATE.READ_LAST_VC;
                    }
                    break;
                case READ_LAST_VC:
                    byte[] lastVC = parseVectorClock(byteBuf, version);
                    if (lastVC != null) {
                        state = STATE.READ_LENGTH;
                    }
                    break;

                case READ_LENGTH:
                    valueLength = parseSigned(byteBuf);
                    if (valueLength != null) {
                        if (valueLength.getLenValue() > 0) {
                            state = STATE.READ_FIELD;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;
                case READ_FIELD:
                    field = parseString(byteBuf);
                    if (field != null) {
                        state = STATE.READ_VALUE;
                    }
                    break;
                case READ_VALUE:
                    value = parseRegister(byteBuf);
                    if (value != null) {
                        propagateCmdIfNeed(field, value);
                        readValueCount++;
                        if (readValueCount >= valueLength.getLenValue()) {
                            state = STATE.READ_END;
                        } else {
                            state = STATE.READ_FIELD;
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

    private void propagateCmdIfNeed(byte[] hashField, byte[] hashValue) {
        if (null == hashField || null == hashValue || null == context.getKey() || context.getCrdtType().isTombstone()) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.HSET,
                new byte[][]{RedisOpType.HSET.name().getBytes(), context.getKey().get(), hashField, hashValue},
                context.getKey(),
                hashField));
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        field = null;
        header = null;
        value = null;
        valueLength = null;
        eof = null;
        readValueCount = 0;
        timestamp = null;
        gid = null;
        maxDel = null;
        state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
