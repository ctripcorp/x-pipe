package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

/**
 * @author TB
 * @date 2026/3/16 14:59
 */
public class RdbFunction2Parser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {
    public RdbFunction2Parser(RdbParseContext context) {

    }

    @Override
    protected Logger getLogger() {
        return null;
    }

    @Override
    public Integer read(ByteBuf byteBuf) {
        return 0;
    }

    @Override
    public boolean isFinish() {
        return false;
    }
}
