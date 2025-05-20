package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public interface FinishParseDataListener {
    void onFinishParse(ByteBuf byteBuf) throws IOException;
}
