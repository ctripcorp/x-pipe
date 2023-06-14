package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/6
 */
public class AuxOnlyRdbParser extends DefaultRdbParser {

    private Logger logger = LoggerFactory.getLogger(AuxOnlyRdbParser.class);

    public AuxOnlyRdbParser() {
    }

    public AuxOnlyRdbParser(RdbParseContext parserManager) {
        super(parserManager);
    }

    @Override
    public boolean isFinish() {
        // RDB end or start reading DB
        return super.isFinish() || RdbParseContext.RdbType.SELECTDB.equals(getContext().getCurrentType());
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
