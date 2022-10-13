package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.redis.core.redis.exception.IntsetParseFailException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2022/6/17
 * refer to https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#intset-encoding
 */
public class Intset {

    private ENC_TYPE encoding;

    private int size;

    private List<Object> data;

    public enum ENC_TYPE {
        INTSET_ENC_INT16(2, ByteBuf::readShortLE),
        INTSET_ENC_INT32(4, ByteBuf::readIntLE),
        INTSET_ENC_INT64(8, ByteBuf::readLongLE);

        private int len;
        private Function<ByteBuf, Object> parser;

        ENC_TYPE(int len, Function<ByteBuf, Object> parser) {
            this.len = len;
            this.parser = parser;
        }

        public Object read(ByteBuf input) {
            return this.parser.apply(input);
        }

        public static ENC_TYPE parse(int enc) {
            for (ENC_TYPE encType: values()) {
                if (encType.len == enc) return encType;
            }

            throw new IntsetParseFailException("unknown encoding " + enc);
        }
    }

    public Intset(byte[] rawData) {
        this.decode(Unpooled.wrappedBuffer(rawData));
    }

    public List<Object> getData() {
        return Collections.unmodifiableList(data);
    }

    public int size() {
        return size;
    }

    public ENC_TYPE getEncoding() {
        return encoding;
    }

    public List<byte[]> convertToStrList() {
        return data.stream().map(val -> String.valueOf(val).getBytes()).collect(Collectors.toList());
    }

    private void decode(ByteBuf input) {
        decodeHeader(input);
        decodeContents(input);
    }

    private void decodeHeader(ByteBuf input) {
        this.encoding = ENC_TYPE.parse(input.readIntLE());
        this.size = input.readIntLE();
    }

    private void decodeContents(ByteBuf input) {
        if (input.readableBytes() != this.encoding.len * this.size) {
            throw new IntsetParseFailException(String.format("intset bytes mismatch enc %s, size %d, bytes %d",
                    encoding.name(), size, input.readableBytes()));
        }

        this.data = new ArrayList<>(size);
        while (input.readableBytes() > 0) {
            this.data.add(encoding.read(input));
        }
    }

}
