package com.ctrip.xpipe.redis.proxy.buffer;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class NativeBufferStore implements BufferStore {

    private ByteToMessageDecoder.Cumulator cumulator = ByteToMessageDecoder.MERGE_CUMULATOR;

    private ByteBuf cumulation;

    private Session session;

    public NativeBufferStore(Session session) {
        this.session = session;
    }

    @Override
    public void offer(ByteBuf byteBuf) {
        boolean first = cumulation == null;
        if(first) {
            cumulation = byteBuf;
        } else {
            cumulator.cumulate(PooledByteBufAllocator.DEFAULT, cumulation, byteBuf);
        }
    }

    @Override
    public synchronized ByteBuf poll() {
        ByteBuf buf = internalBuffer();
//        int readable = buf.readableBytes();
//        if (readable > 0) {
//            result = buf.readBytes(readable);
//            buf.release();
//        } else {
//            buf.release();
//        }
        cumulation = null;
        return buf;
    }

    @Override
    public boolean isEmpty() {
        return !(cumulation != null && cumulation.isReadable());
    }

    @Override
    public void clearAndSend(Channel channel) {
        if(channel == null) {
            channel = session.getChannel();
        }
        channel.writeAndFlush(poll());
    }

    @Override
    public void release() throws Exception {
        if(cumulation != null) {
            cumulation.release();
            cumulation = null;
        }
    }

    @VisibleForTesting
    protected ByteBuf internalBuffer() {
        if (cumulation != null) {
            return cumulation;
        } else {
            return Unpooled.EMPTY_BUFFER;
        }
    }
}
