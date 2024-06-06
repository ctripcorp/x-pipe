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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hailu
 * @date 2024/5/24 10:38
 */
public class DefaultRdbCrdtParser extends AbstractRdbParser<byte[]> implements RdbParser<byte[]> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private RdbLength typeId;

    private ByteBuf temp;

    private byte[] val;

    private static final Logger logger = LoggerFactory.getLogger(DefaultRdbCrdtParser.class);

    enum STATE {
        READ_INIT,
        READ_TYPE,
        READ_HEAD,
        READ_GID,
        READ_TIMESTAMP,
        READ_VECTOR_CLOCK,
        READ_VAL,
        READ_EOF,
        READ_END
    }

    public DefaultRdbCrdtParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    typeId = null;
                    val = null;
                    state = STATE.READ_TYPE;
                    break;
                case READ_TYPE:
                    typeId = parseRdbLength(byteBuf);
                    if (typeId != null) {
                        state = STATE.READ_VAL;
                    }
                    break;

                case READ_VAL:
                    RdbParseContext.RdbCrdtType crdtType = RdbParseContext.RdbCrdtType.findByTypeId(typeId.getLenLongValue());
                    context.setCrdtType(crdtType);
                    RdbParser<byte[]> subParser = (RdbParser<byte[]>)context.getOrCreateCrdtParser(crdtType);
                    if (subParser == null) {
                        throw new RdbParseFailException("crdt rdb parse error" + typeId.getLenLongValue());
                    }
                    val = subParser.read(byteBuf);
                    if (subParser.isFinish()) {
                        subParser.reset();
                        context.clearKvContext();
                        context.clearCrdtType();
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

    }
    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        if (temp != null) {
            temp.release();
            temp = null;
        }
        this.state = STATE.READ_INIT;
        typeId = null;
        val = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
