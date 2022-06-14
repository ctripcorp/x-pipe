package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/9
 */
public class RdbIdleParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private RdbLength idle;

    private static final Logger logger = LoggerFactory.getLogger(RdbIdleParser.class);

    enum STATE {
        READ_INIT,
        READ_IDLE,
        READ_END
    }

    public RdbIdleParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    idle = null;
                    state = STATE.READ_IDLE;
                    break;

                case READ_IDLE:
                    idle = parseRdbLength(byteBuf);
                    if (null != idle) {
                        this.context.setLruIdle(idle.getLenValue());
                        state = STATE.READ_END;
                    }
                    break;

                case READ_END:
                default:

            }

        }

        if (null != idle) return idle.getLenValue();
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
