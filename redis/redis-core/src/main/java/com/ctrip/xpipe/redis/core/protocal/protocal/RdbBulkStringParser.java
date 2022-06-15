package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdbBulkStringParser extends AbstractBulkStringParser {
    
    private static final Logger logger = LoggerFactory.getLogger(RdbBulkStringParser.class);

    private RdbParser<?> rdbParser;
    
    @Override
    protected Logger getLogger() {
        return logger;
    }

    public RdbBulkStringParser(InOutPayload payload, RdbParser<?> rdbParser) {
        super(payload);
        this.rdbParser = rdbParser;
    }

    public RdbBulkStringParser(InOutPayload payload) {
        this(payload, null);
    }

    @Override
    protected int readContent(ByteBuf byteBuf) {
        if (null != rdbParser && !rdbParser.isFinish()) rdbParser.read(byteBuf.slice());
        return super.readContent(byteBuf);
    }

    public RdbBulkStringParser(String content) {
        super(new StringInOutPayload(content));
    }

    @Override
    protected RedisClientProtocol<InOutPayload> readEnd(ByteBuf byteBuf) {
        return new RdbBulkStringParser(payload);
    }
}
