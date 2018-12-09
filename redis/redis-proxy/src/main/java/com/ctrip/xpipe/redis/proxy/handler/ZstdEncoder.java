package com.ctrip.xpipe.redis.proxy.handler;

import com.github.luben.zstd.ZstdDirectBufferCompressingStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ZstdEncoder extends MessageToByteEncoder<ByteBuf> {



    private void init() {

    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {

    }

    private class ZstdCompressStream extends ZstdDirectBufferCompressingStream {

        public ZstdCompressStream(ByteBuffer byteBuffer, int i) throws IOException {
            super(byteBuffer, i);
        }

        @Override
        protected ByteBuffer flushBuffer(ByteBuffer byteBuffer) throws IOException {
            return super.flushBuffer(byteBuffer);
        }
    }
}
