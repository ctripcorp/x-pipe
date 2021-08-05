package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdbBulkStringParser extends AbstractBulkStringParser{
    private static Logger logger = LoggerFactory.getLogger(CommandBulkStringParser.class);

    public RdbBulkStringParser(InOutPayload payload) {
        super(payload);
    }

    public RdbBulkStringParser(String content) {
        super(new StringInOutPayload(content));
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    boolean readContent(ByteBuf byteBuf) {
         return readPayload(byteBuf);
    }

    @Override
    RedisClientProtocol<InOutPayload> getResult() {
        return new RdbBulkStringParser(payload);
    }

}
