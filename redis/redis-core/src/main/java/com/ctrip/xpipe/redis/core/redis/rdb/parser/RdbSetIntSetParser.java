package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Intset;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/6/18
 */
public class RdbSetIntSetParser extends AbstractRdbParser<List<byte[]>> implements RdbParser<List<byte[]>> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private Intset intset;

    private byte[] temp;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbSetIntSetParser.class);

    enum STATE {
        READ_INIT,
        READ_AS_STRING,
        DECODE_INTSET,
        READ_END
    }

    public RdbSetIntSetParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) parseContext.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public List<byte[]> read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    intset = null;
                    temp = null;
                    state = STATE.READ_AS_STRING;
                    break;

                case READ_AS_STRING:
                    temp = rdbStringParser.read(byteBuf);
                    if (null == temp) {
                        break;
                    }
                    rdbStringParser.reset();
                    state = STATE.DECODE_INTSET;

                case DECODE_INTSET:
                    intset = new Intset(temp);
                    state = STATE.READ_END;

                case READ_END:
                default:
                    temp = null;

            }

            if (isFinish()) {
                propagateCmdIfNeed(intset);
                propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
            }
        }

        if (isFinish()) return intset.convertToStrList();
        else return null;
    }

    private void propagateCmdIfNeed(Intset intset) {
        if (null == context.getKey() || null == intset) return;

        List<byte[]> vals = intset.convertToStrList();
        RedisKey key = context.getKey();
        for (byte[] val: vals) {
            notifyRedisOp(new RedisOpSingleKey(
                    RedisOpType.SADD,
                    new byte[][] {RedisOpType.SADD.name().getBytes(), key.get(), val},
                    key, val));
        }
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
        this.intset = null;
        this.temp = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
