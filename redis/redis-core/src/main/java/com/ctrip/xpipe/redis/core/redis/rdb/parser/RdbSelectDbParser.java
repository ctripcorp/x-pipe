package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSelect;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/4
 */
public class RdbSelectDbParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private RdbLength dbId;

    private static final Logger logger = LoggerFactory.getLogger(RdbSelectDbParser.class);

    enum STATE {
        READ_INIT,
        READ_DBID,
        READ_END
    }

    public RdbSelectDbParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    dbId = null;
                    state = STATE.READ_DBID;
                    break;

                case READ_DBID:
                    dbId = parseRdbLength(byteBuf);
                    if (null != dbId) {
                        this.context.setDbId(dbId.getLenValue());
                        notifyRedisOp(new RedisOpSelect(dbId.getLenValue()));
                        state = STATE.READ_END;
                    }
                    break;

                case READ_END:
                default:
            }

        }

        if (isFinish()) return dbId.getLenValue();
        return null;
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        this.state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
