package com.ctrip.xpipe.redis.proxy.buffer;

import com.ctrip.xpipe.redis.proxy.exception.ResourceNotFoundException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class SimpleBlockingQueueBufferStore implements BufferStore {

    private BlockingDeque<ByteBuf> buffer = new LinkedBlockingDeque<>();

    @Override
    public void offer(ByteBuf byteBuf) {
        if(buffer != null) {
            buffer.offer(byteBuf.retain());
        } else {
            throw new ResourceNotFoundException("empty buffer");
        }
    }

    @Override
    public ByteBuf poll() {
        return buffer.poll();
    }

    @Override
    public boolean isEmpty() {
        return buffer == null || buffer.isEmpty();
    }

    @Override
    public void clearAndSend(Channel channel) {
        synchronized (buffer) {
            while(!buffer.isEmpty()) {
                ByteBuf byteBuf = buffer.poll();
                channel.write(byteBuf);
                byteBuf.release();
            }
            channel.flush();
        }
    }

    @Override
    public void release() throws Exception {
        if(buffer != null) {
            buffer.clear();
            buffer = null;
        }
    }

}
