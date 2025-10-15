package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandLister;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class StreamCommandReader implements StreamCommandLister {

    private long currentOffset;
    private long lastOffset = -1;

    private DefaultIndexStore defaultIndexStore;

    private StreamCommandParser streamCommandParser;

    public StreamCommandReader(DefaultIndexStore defaultIndexStore, long offset) {
        this.defaultIndexStore = defaultIndexStore;
        this.currentOffset = offset;
        this.lastOffset = offset;
        streamCommandParser = new StreamCommandParser(this);
    }

    public void doRead(ByteBuf byteBuf) throws IOException {
        streamCommandParser.doRead(byteBuf);
    }

    public void resetOffset() {
        this.currentOffset = 0;
        this.lastOffset = 0;
    }

    private String readGtid(Object[] payload) {
        if(payload == null || payload.length <= 2) {
            return null;
        }
        if(StringUtil.trimEquals("GTID", payload[0].toString())) {
            return payload[1].toString();
        } else {
            return null;
        }

    }

    public void resetParser() {
        streamCommandParser.reset();
    }

    public int getRemainLength() {
        return streamCommandParser.getRemainLength();
    }

    @Override
    public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        String gtid = readGtid(payload);
        boolean needWrite = true;
        if (!StringUtil.isEmpty(gtid)) {
            // 写索引
            needWrite &= defaultIndexStore.onCommand(gtid, this.lastOffset);
        }
        if(needWrite) {
            // 写cmd文件
            this.currentOffset += commandBuf.readableBytes();
            defaultIndexStore.onFinishParse(commandBuf);
        }
        lastOffset = this.currentOffset;
    }
}
