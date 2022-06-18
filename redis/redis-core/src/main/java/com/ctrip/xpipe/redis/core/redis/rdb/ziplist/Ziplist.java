package com.ctrip.xpipe.redis.core.redis.rdb.ziplist;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.exception.ZiplistParseFailException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2022/6/16
 */
public class Ziplist {

    private List<ZiplistEntry> entries;

    private long totalBytes;

    private long tail;

    private int size;

    public Ziplist(byte[] rawData) {
        this.decode(Unpooled.wrappedBuffer(rawData));
    }

    public List<ZiplistEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    public int size() {
        return size;
    }

    public List<byte[]> convertToList() {
        return entries.stream().map(ZiplistEntry::getBytes).collect(Collectors.toList());
    }

    public Map<byte[], byte[]> convertToMap() {
        if (0 != size % 2) {
            throw new XpipeRuntimeException("ziplist size " + size + " can't convert to map");
        }

        Map<byte[], byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < size; i += 2) {
            map.put(entries.get(i).getBytes(), entries.get(i + 1).getBytes());
        }

        return map;
    }

    private void decode(ByteBuf rawData) {
        int readableBytes = rawData.readableBytes();
        decodeHeader(rawData);
        if (readableBytes != totalBytes) {
            throw new ZiplistParseFailException("ziplist bytes mismatch expect " + totalBytes + " but " + readableBytes);
        }

        this.entries = new ArrayList<>(size);

        while (rawData.readableBytes() > 0 && (byte)0xff != rawData.getByte(rawData.readerIndex())) {
            entries.add(ZiplistEntry.parse(rawData));
        }

        if (entries.size() != size) {
            throw new ZiplistParseFailException("ziplist entry size mismatch expect " + size + " but " + entries.size());
        }
    }

    private void decodeHeader(ByteBuf input) {
        this.totalBytes = input.readUnsignedIntLE();
        this.tail = input.readUnsignedIntLE();
        this.size = input.readUnsignedShortLE();
    }

}
