package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;

public class RdbBulkStringParser extends AbstractBulkStringParser {

    public RdbBulkStringParser(InOutPayload payload) {
        super(payload);
    }

    public RdbBulkStringParser(String content) {
        super(new StringInOutPayload(content));
    }

    @Override
    protected RedisClientProtocol<InOutPayload> readEnd(ByteBuf byteBuf) {
        return new RdbBulkStringParser(payload);
    }
}
