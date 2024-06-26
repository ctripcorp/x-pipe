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
public class RdbCrdtRcParser extends AbstractRdbCrdtParser<byte[]> implements RdbParser<byte[]> {
    private STATE state = STATE.READ_INIT;
    private RdbLength header;
    private int version;
    private RdbLength eof;
    private byte[] val;

    private static final Logger logger = LoggerFactory.getLogger(RdbCrdtRcParser.class);

    enum STATE {
        READ_INIT,
        READ_HEAD,
        READ_VAL,
        READ_EOF,
        READ_END
    }

    public RdbCrdtRcParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    header = null;
                    val = null;
                    eof = null;
                    state = STATE.READ_HEAD;
                    break;
                case READ_HEAD:
                    header = parseSigned(byteBuf);
                    if (header != null) {
                        int type = (int) header.getLenLongValue() & ((1 << 8) - 1);
                        version = ((int) (header.getLenLongValue() >> 48) & ((1 << 16) - 1));
                        if (type == ORSET_TYPE && version >= 1) {
                            state = STATE.READ_VAL;
                        } else {
                            state = STATE.READ_EOF;
                        }
                    }
                    break;

                case READ_VAL:
                    val = parseRc(byteBuf);
                    if (val != null) {
                        state = STATE.READ_END;
                    }
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

    private void propagateCmdIfNeed() {
        if (null == val
                || null == context.getKey()
                || !RdbParseContext.RdbType.CRDT.equals(context.getCurrentType())
                || context.getCrdtType().isTombstone()) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.SET,
                new byte[][]{RedisOpType.SET.name().getBytes(), context.getKey().get(), val},
                context.getKey(), val));
        propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
    }


    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        val = null;
        header = null;
        eof = null;
        state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
