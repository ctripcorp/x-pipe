package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.redis.core.redis.exception.ListpackParseFailException;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lishanglin
 * date 2022/6/18
 */
public class ListpackEntry {

    private long length;

    private AtomicLong intVal;

    private byte[] data;

    private static final byte LP_ENCODING_7BIT_UINT = 0;
    private static final byte LP_ENCODING_7BIT_UINT_MASK = (byte)0x80;
    private static final byte LP_ENCODING_6BIT_STR = (byte)0x80;
    private static final byte LP_ENCODING_6BIT_STR_MASK = (byte)0xC0;
    private static final byte LP_ENCODING_13BIT_INT = (byte)0xC0;
    private static final byte LP_ENCODING_13BIT_INT_MASK = (byte)0xE0;
    private static final byte LP_ENCODING_16BIT_INT = (byte)0xF1;
    private static final byte LP_ENCODING_16BIT_INT_MASK = (byte)0xFF;
    private static final byte LP_ENCODING_24BIT_INT = (byte)0xF2;
    private static final byte LP_ENCODING_24BIT_INT_MASK = (byte)0xFF;
    private static final byte LP_ENCODING_32BIT_INT = (byte)0xF3;
    private static final byte LP_ENCODING_32BIT_INT_MASK = (byte)0xFF;
    private static final byte LP_ENCODING_64BIT_INT = (byte)0xF4;
    private static final byte LP_ENCODING_64BIT_INT_MASK = (byte)0xFF;
    private static final byte LP_ENCODING_12BIT_STR = (byte)0xE0;
    private static final byte LP_ENCODING_12BIT_STR_MASK = (byte)0xF0;
    private static final byte LP_ENCODING_32BIT_STR = (byte)0xF0;
    private static final byte LP_ENCODING_32BIT_STR_MASK = (byte)0xFF;


    public static ListpackEntry parse(ByteBuf input) {
        return new ListpackEntry(input);
    }

    private ListpackEntry(ByteBuf input) {
        decode(input);
        skipTotalLen(input);
    }

    public byte[] getBytes() {
        return data;
    }

    public long getInt() {
        if (null == intVal) throw new ListpackParseFailException("not int val but:" + new String(data));
        return intVal.get();
    }

    private void decode(ByteBuf input) {
        byte encoding = input.readByte();
        int count;
        if (checkEncoding(encoding, LP_ENCODING_7BIT_UINT_MASK, LP_ENCODING_7BIT_UINT)) {
            setInt(encoding);
            this.length = 1;
        } else if (checkEncoding(encoding, LP_ENCODING_6BIT_STR_MASK, LP_ENCODING_6BIT_STR)) {
            this.readStr(input, encoding & 0x3F);
            this.length = 1L + this.data.length;
        } else if (checkEncoding(encoding, LP_ENCODING_13BIT_INT_MASK, LP_ENCODING_13BIT_INT)) {
            this.setSignedInt(((encoding & 0x1f) << 8) | (input.readByte() & 0xff), 1<<12, 8191);
            this.length = 2;
        } else if (checkEncoding(encoding, LP_ENCODING_16BIT_INT_MASK, LP_ENCODING_16BIT_INT)) {
            this.setInt(input.readShortLE());
            this.length = 3;
        } else if (checkEncoding(encoding, LP_ENCODING_24BIT_INT_MASK, LP_ENCODING_24BIT_INT)) {
            input.readerIndex(input.readerIndex() - 1);
            int val = input.readIntLE();
            this.setInt(val >> 8);
            this.length = 4;
        } else if (checkEncoding(encoding, LP_ENCODING_32BIT_INT_MASK, LP_ENCODING_32BIT_INT)) {
            this.setInt(input.readIntLE());
            this.length = 5;
        } else if (checkEncoding(encoding, LP_ENCODING_64BIT_INT_MASK, LP_ENCODING_64BIT_INT)) {
            this.setInt(input.readLongLE());
            this.length = 9;
        } else if (checkEncoding(encoding, LP_ENCODING_12BIT_STR_MASK, LP_ENCODING_12BIT_STR)) {
            count = ((encoding & 0xF) << 8) | (input.readByte() & 0xff);
            this.readStr(input, count);
            this.length = 2L + this.data.length;
        }  else if (checkEncoding(encoding, LP_ENCODING_32BIT_STR_MASK, LP_ENCODING_32BIT_STR)) {
            count = input.readIntLE();
            this.readStr(input, count);
            this.length = 5L + this.data.length;
        } else {
            throw new ListpackParseFailException("unknown encoding " + encoding);
        }
    }

    private boolean checkEncoding(byte encoding, byte mask, byte unit) {
        return (byte)(encoding & mask) == unit;
    }

    private void setInt(long val) {
        this.intVal = new AtomicLong(val);
        this.data = String.valueOf(val).getBytes();
    }

    private void setSignedInt(long val, long negstart, long negmax) {
        if (val >= negstart) {
            setInt(-(negmax - val) - 1);
        } else {
            setInt(val);
        }
    }

    private void readStr(ByteBuf input, int size) {
        byte[] data = new byte[size];
        input.readBytes(data);
        this.data = data;
    }

    private void skipTotalLen(ByteBuf byteBuf) {
        int skip;
        if (length <= 127) {
            skip = 1;
        } else if (length <= 16383) {
            skip = 2;
        } else if (length <= 2097151) {
            skip = 3;
        } else if (length <= 268435455) {
            skip = 4;
        } else {
            skip = 5;
        }

        byteBuf.readerIndex(byteBuf.readerIndex() + skip);
    }


}
