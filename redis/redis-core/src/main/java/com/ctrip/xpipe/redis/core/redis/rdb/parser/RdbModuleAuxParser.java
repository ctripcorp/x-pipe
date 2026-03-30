package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author TB
 * @date 2026/3/16 15:00
 */
public class RdbModuleAuxParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(RdbModuleAuxParser.class);

    public RdbModuleAuxParser(RdbParseContext context) {
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public Integer read(ByteBuf byteBuf) {
        return 0;
    }

    @Override
    public boolean isFinish() {
        return true;
    }
}
