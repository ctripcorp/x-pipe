package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/5/29
 */
public class RdbStringParser extends AbstractRdbParser implements RdbParser {

    private static final Logger logger = LoggerFactory.getLogger(RdbAuxParser.class);

    public RdbStringParser() {

    }

    public RdbStringParser(RdbParseContext parseContext) {
    }

    @Override
    public void read(ByteBuf byteBuf) {

    }

    @Override
    public boolean isFinish() {
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
