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
public class RdbExpiretimeParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private Integer expiretimeSecond;

    private ByteBuf temp;

    private static final Logger logger = LoggerFactory.getLogger(RdbExpiretimeParser.class);

    enum STATE {
        READ_INIT,
        READ_EXPIRETIME,
        READ_END
    }

    public RdbExpiretimeParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    expiretimeSecond = null;
                    temp = null;
                    state = STATE.READ_EXPIRETIME;
                    break;

                case READ_EXPIRETIME:
                    temp = readUntilBytesEnough(byteBuf, temp, 4);
                    if (temp.readableBytes() == 4) {
                        expiretimeSecond = temp.readInt();
                        context.setExpireMilli(expiretimeSecond * 1000L);
                        state = STATE.READ_END;
                    }
                    break;

                case READ_END:
                default:

            }

        }

        return expiretimeSecond;
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
