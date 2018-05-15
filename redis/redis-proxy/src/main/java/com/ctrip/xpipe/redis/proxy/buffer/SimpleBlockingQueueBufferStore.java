package com.ctrip.xpipe.redis.proxy.buffer;

import com.ctrip.xpipe.redis.proxy.Session;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class SimpleBlockingQueueBufferStore implements BufferStore {

    private static final Logger logger = LoggerFactory.getLogger(SimpleBlockingQueueBufferStore.class);

    private Session session;

    private BlockingDeque<ByteBuf> buffer = new LinkedBlockingDeque<>();

    public SimpleBlockingQueueBufferStore(Session session) {
        this.session = session;
    }

    @Override
    public void offer(ByteBuf byteBuf) {
        if(buffer != null) {
            buffer.offer(byteBuf);
        } else {
            logger.error("[offer] empty buffer");
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
    public void clearAndSend() {
        synchronized (buffer) {
            BlockingDeque<ByteBuf> clone = buffer;
            buffer = null;
            while(!clone.isEmpty()) {
                session.tryWrite(clone.poll());
            }
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
