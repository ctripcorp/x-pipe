package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.exception.ListpackParseFailException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2022/6/18
 * refer to https://github.com/antirez/listpack/blob/master/listpack.md
 */
public class Listpack {

    private long totalBytes;

    private int size;

    private List<ListpackEntry> elements;

    public Listpack(byte[] rawData) {
        this.decode(Unpooled.wrappedBuffer(rawData));
    }

    public int size() {
        return size;
    }

    public byte[] get(int index) {
        return elements.get(index).getBytes();
    }

    public ListpackEntry getElement(int index) {
        return elements.get(index);
    }

    public List<ListpackEntry> getElements() {
        return Collections.unmodifiableList(elements);
    }

    public List<byte[]> convertToList() {
        return elements.stream().map(ListpackEntry::getBytes).collect(Collectors.toList());
    }

    public Map<byte[], byte[]> convertToMap() {
        if (0 != size % 2) {
            throw new XpipeRuntimeException("listpack size " + size + " can't convert to map");
        }

        Map<byte[], byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < size; i += 2) {
            map.put(elements.get(i).getBytes(), elements.get(i + 1).getBytes());
        }

        return map;
    }

    private void decode(ByteBuf input) {
        decodeHeader(input);
        decodeElements(input);
    }

    private void decodeHeader(ByteBuf input) {
        int readableBytes = input.readableBytes();
        this.totalBytes = input.readUnsignedIntLE();
        this.size = input.readUnsignedShortLE();

        if (readableBytes != totalBytes) {
            throw new ListpackParseFailException("listpack bytes mismatch exp " + totalBytes + " but " + readableBytes);
        }
    }

    private void decodeElements(ByteBuf input) {
        this.elements = new ArrayList<>(size);

        while (input.readableBytes() > 0 && (byte)0xff != input.getByte(input.readerIndex())) {
            this.elements.add(ListpackEntry.parse(input));
        }

        if (elements.size() != size) {
            throw new ListpackParseFailException("listpack elements size mismatch expect " + size + " but " + elements.size());
        }
    }

}
