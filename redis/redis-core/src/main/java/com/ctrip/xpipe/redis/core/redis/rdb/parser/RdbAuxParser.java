package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/5/29
 */
public class RdbAuxParser extends AbstractRdbParser<Pair<String, String>> implements RdbParser<Pair<String, String>> {

    private RdbParseContext context;

    private RdbParser<?> rdbStringParser;

    private STATE state = STATE.READ_INIT;

    private String key;

    private String value;

    private static final Logger logger = LoggerFactory.getLogger(RdbAuxParser.class);

    enum STATE {
        READ_INIT,
        READ_KEY,
        READ_VALUE,
        READ_END,
    }

    public RdbAuxParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = parseContext.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Pair<String, String> read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    key = null;
                    value = null;
                    state = STATE.READ_KEY;
                    break;

                case READ_KEY:
                    Object rawKey = rdbStringParser.read(byteBuf);
                    if (null != rawKey) {
                        key = decodeRawStr(rawKey);
                        rdbStringParser.reset();
                        state = STATE.READ_VALUE;
                    }
                    break;

                case READ_VALUE:
                    Object rawValue = rdbStringParser.read(byteBuf);
                    if (null != rawValue) {
                        value = decodeRawStr(rawValue);
                        rdbStringParser.reset();
                        context.setAux(key, value);
                        notifyAux(key, value);
                        state = STATE.READ_END;
                    }
                    break;

                case READ_END:
                default:
            }
        }

        if (isFinish()) return Pair.of(key, value);
        return null;
    }

    private String decodeRawStr(Object rawStr) {
        if (rawStr instanceof byte[]) {
            return new String((byte[]) rawStr, Codec.defaultCharset);
        } else return rawStr.toString();
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
        this.key = null;
        this.value = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
