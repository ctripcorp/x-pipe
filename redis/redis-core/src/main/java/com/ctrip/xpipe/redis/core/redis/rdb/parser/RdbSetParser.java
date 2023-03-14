package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
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
public class RdbSetParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private RdbLength len;

    private int readCnt;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbSetParser.class);

    enum STATE {
        READ_INIT,
        READ_LEN,
        READ_VALUE,
        READ_END
    }

    public RdbSetParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    len = null;
                    readCnt = 0;
                    state = STATE.READ_LEN;
                    break;

                case READ_LEN:
                    len = parseRdbLength(byteBuf);
                    if (null != len) {
                        if (len.getLenValue() > 0) {
                            state = STATE.READ_VALUE;
                        } else {
                            throw new RdbParseEmptyKeyException("set key " + context.getKey());
                        }
                    }
                    break;

                case READ_VALUE:
                    byte[] value = rdbStringParser.read(byteBuf);
                    if (null != value) {
                        rdbStringParser.reset();
                        propagateCmdIfNeed(value);

                        readCnt++;
                        if (readCnt >= len.getLenValue()) {
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

        if (isFinish()) return len.getLenValue();
        else return null;
    }

    private void propagateCmdIfNeed(byte[] value) {
        if (null == value || null == context.getKey()) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.SADD,
                new byte[][] {RedisOpType.SADD.name().getBytes(), context.getKey().get(), value},
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
        this.state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
