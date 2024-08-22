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

/**
 * @author lishanglin
 * date 2022/6/18
 */
public class RdbBitmapParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private RdbLength bitmapLength;

    private RdbLength subkeyLength;

    private int readLength;

    private byte[] val;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbBitmapParser.class);

    enum STATE {
        READ_INIT,
        READ_BITMAP_LEN,
        READ_SUBKEY_LEN,
        READ_VALUE,
        READ_END
    }

    public RdbBitmapParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_INIT:
                    bitmapLength = null;
                    subkeyLength = null;
                    val = null;
                    readLength = 0;
                    state = STATE.READ_BITMAP_LEN;
                    break;

                case READ_BITMAP_LEN:
                    bitmapLength = parseRdbLength(byteBuf);
                    if (null != bitmapLength) {
                        if (bitmapLength.getLenValue() < 0) {
                            throw new RdbParseFailException("bitmap key " + context.getKey());
                        }
                        val = new byte[bitmapLength.getLenValue()];
                        state = STATE.READ_SUBKEY_LEN;
                    }
                    break;
                case READ_SUBKEY_LEN:
                    subkeyLength = parseRdbLength(byteBuf);
                    if (null != subkeyLength) {
                        if (subkeyLength.getLenValue() <= 0) {
                            throw new RdbParseFailException("bitmap key " + context.getKey());
                        }
                        state = STATE.READ_VALUE;
                    }
                    break;
                case READ_VALUE:
                    byte[] value = rdbStringParser.read(byteBuf);
                    if (null != value) {
                        rdbStringParser.reset();
                        System.arraycopy(value, 0, val, readLength, value.length);
                        readLength += value.length;
                        if (readLength >= bitmapLength.getLenValue()) {
                            propagateCmdIfNeed(val);
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

        if (isFinish()) return bitmapLength.getLenValue();
        else return null;
    }

    private void propagateCmdIfNeed(byte[] value) {
        if (null == value || null == context.getKey()) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.SET,
                new byte[][]{RedisOpType.SET.name().getBytes(), context.getKey().get(), value},
                context.getKey(), value));
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        if (rdbStringParser != null) {
            rdbStringParser.reset();
        }
        bitmapLength = null;
        subkeyLength = null;
        val = null;
        readLength = 0;
        this.state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
