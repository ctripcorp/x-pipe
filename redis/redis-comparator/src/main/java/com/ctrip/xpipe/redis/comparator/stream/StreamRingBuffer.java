package com.ctrip.xpipe.redis.comparator.stream;

import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import io.netty.buffer.ByteBuf;

import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * 单路流有界 RingBuffer（spec D34 / D24）。
 * <p>
 * 堆内 {@code byte[]}，单路容量固定为构造时传入的字节数（生产取
 * {@code comparator.stream.buffer.bytes}，不随路数 / 分片数缩放）。
 * 写满覆盖最旧；写侧永不阻塞、永不关 autoRead。读位点由调用方
 * {@code comparedEnd} 表达，本类不提供 {@code skip} / {@code skipUntil}。
 * <p>
 * 单写单读：即将覆盖时先 {@code volatile} 推进 {@code bufferStart}，再写入
 * 槽位，最后发布 {@code receivedEnd}。读侧只读已发布区间，拷贝后再核
 * {@code bufferStart}，避免读到正在覆盖的槽。
 */
public final class StreamRingBuffer {

    public enum PeekStatus {
        HIT,
        OVERWRITTEN,
        NOT_YET
    }

    private final int capacity;

    private final long streamStart;

    private final byte[] data;

    private volatile long receivedEnd;

    private volatile long bufferStart;

    public StreamRingBuffer(ComparatorConfig config, long streamStart) {
        this(Objects.requireNonNull(config, "config").getStreamBufferBytes(), streamStart);
    }

    public StreamRingBuffer(int capacity, long streamStart) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.streamStart = streamStart;
        this.data = new byte[capacity];
        this.receivedEnd = streamStart;
        this.bufferStart = streamStart;
    }

    /**
     * 拷贝 inbound 到自有 chunk。禁止 retain / 钉住 Direct；不改 autoRead。
     */
    public void write(ByteBuf buf) {
        Objects.requireNonNull(buf, "buf");
        int remaining = buf.readableBytes();
        if (remaining == 0) {
            return;
        }
        long pos = receivedEnd;
        long newEnd = pos + remaining;
        long newStart = Math.max(streamStart, newEnd - capacity);
        if (newStart > bufferStart) {
            bufferStart = newStart;
            VarHandle.storeStoreFence();
        }
        int srcIndex = buf.readerIndex();
        while (remaining > 0) {
            int phys = physical(pos);
            int chunk = Math.min(remaining, capacity - phys);
            buf.getBytes(srcIndex, data, phys, chunk);
            srcIndex += chunk;
            pos += chunk;
            remaining -= chunk;
        }
        receivedEnd = newEnd;
    }

    /**
     * 命中 {@code offset >= bufferStart && offset + len <= receivedEnd} 才拷进 {@code reuse}。
     * 跨环回绕在本方法内拼好，不分配临时 {@code ByteBuf}。
     */
    public PeekStatus peek(long offset, int len, byte[] reuse) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be non-negative: " + len);
        }
        Objects.requireNonNull(reuse, "reuse");
        if (reuse.length < len) {
            throw new IllegalArgumentException("reuse too small: " + reuse.length + " < " + len);
        }
        long end = receivedEnd;
        long start = bufferStart;
        if (offset < start) {
            return PeekStatus.OVERWRITTEN;
        }
        if (offset > end || len > end - offset) {
            return PeekStatus.NOT_YET;
        }
        if (len == 0) {
            return PeekStatus.HIT;
        }
        int phys = physical(offset);
        int first = Math.min(len, capacity - phys);
        System.arraycopy(data, phys, reuse, 0, first);
        if (first < len) {
            System.arraycopy(data, 0, reuse, first, len - first);
        }
        VarHandle.loadLoadFence();
        if (offset < bufferStart) {
            return PeekStatus.OVERWRITTEN;
        }
        return PeekStatus.HIT;
    }

    public long getReceivedEnd() {
        return receivedEnd;
    }

    public long getBufferStart() {
        return bufferStart;
    }

    public int getCapacity() {
        return capacity;
    }

    public long getStreamStart() {
        return streamStart;
    }

    private int physical(long offset) {
        return (int) ((offset - streamStart) % capacity);
    }
}
