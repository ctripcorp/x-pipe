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
public class RdbFreqParser extends AbstractRdbParser<Short> implements RdbParser<Short> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private Short freq;

    private static final Logger logger = LoggerFactory.getLogger(RdbFreqParser.class);

    enum STATE {
        READ_INIT,
        READ_FREQ,
        READ_END
    }

    public RdbFreqParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public Short read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    freq = null;
                    state = STATE.READ_FREQ;
                    break;

                case READ_FREQ:
                    freq = byteBuf.readUnsignedByte();
                    this.context.setLfuFreq(freq);
                    state = STATE.READ_END;
                    break;

                case READ_END:
                default:

            }

        }

        return freq;
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
