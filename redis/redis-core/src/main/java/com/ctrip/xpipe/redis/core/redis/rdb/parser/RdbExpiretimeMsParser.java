package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/9
 */
public class RdbExpiretimeMsParser extends AbstractRdbParser<Long> implements RdbParser<Long> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private Long expiretimeMillisecond;

    private static final Logger logger = LoggerFactory.getLogger(RdbExpiretimeMsParser.class);

    enum STATE {
        READ_INIT,
        READ_EXPIRETIME,
        READ_END
    }

    public RdbExpiretimeMsParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public Long read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    expiretimeMillisecond = null;
                    state = STATE.READ_EXPIRETIME;
                    break;

                case READ_EXPIRETIME:
                    expiretimeMillisecond = readMillSecond(byteBuf, context);
                    if (null != expiretimeMillisecond) {
                        context.setExpireMilli(expiretimeMillisecond);
                        state = STATE.READ_END;
                    }
                    break;

                case READ_END:
                default:

            }

        }

        return expiretimeMillisecond;
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        this.state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
