package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.redis.core.redis.exception.ZiplistParseFailException;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/16
 */
public class ZiplistEntry {

    private long prevLength;

    private int encoding;

    private int length;

    private byte[] data;

    private static final int ZIP_BIG_PREV_LEN = 254;

    private static final byte ZIP_STR_MASK = (byte) 0xc0;
    private static final byte ZIP_INT_MASK = (byte) 0x30;

    private static final byte ZIP_STR_06B = (byte) 0x00;
    private static final byte ZIP_STR_14B = (byte) 0x40;
    private static final byte ZIP_STR_32B = (byte) 0x80;
    private static final byte ZIP_INT_16B = (byte) 0xc0;
    private static final byte ZIP_INT_32B = (byte) 0xd0;
    private static final byte ZIP_INT_64B = (byte) 0xe0;
    private static final byte ZIP_INT_24B = (byte) 0xf0;
    private static final byte ZIP_INT_8B = (byte) 0xfe;

    private static final byte ZIP_INT_IMM_MASK = (byte) 0x0f;
    private static final int ZIP_INT_IMM_MIN = 0xf1;
    private static final int ZIP_INT_IMM_MAX = 0xfd;

    private Logger logger = LoggerFactory.getLogger(ZiplistEntry.class);

    private ZiplistEntry(ByteBuf input) {
        decodePrevLen(input);
        decodeFlag(input);
        decodeRawBytes(input);
    }

    public byte[] getBytes() {
        return data;
    }

    public static ZiplistEntry parse(ByteBuf input) {
        return new ZiplistEntry(input);
    }

    private void decodePrevLen(ByteBuf input) {
        short len = input.readUnsignedByte();
        if (ZIP_BIG_PREV_LEN > len) {
            this.prevLength = len;
        } else if (ZIP_BIG_PREV_LEN == len) {
            this.prevLength = input.readUnsignedInt();
        } else {
            throw new ZiplistParseFailException("prev len over big prev len: " + len);
        }
    }

    private void decodeFlag(ByteBuf input) {
        byte flag = input.getByte(input.readerIndex());
        if (((byte)(flag & ZIP_STR_MASK)) != ZIP_STR_MASK) {
            this.encoding = ZIP_STR_MASK & flag;
        } else {
            this.encoding = flag;
        }
    }

    private void decodeRawBytes(ByteBuf input) {
        if (ZIP_STR_06B == encoding) {
            this.length = input.readByte() & 0x3f;
            readStr(input, this.length);
        } else if (ZIP_STR_14B == encoding) {
            this.length = ((input.readByte() & 0x3f) << 8) | input.readUnsignedByte();
            readStr(input, this.length);
        } else if (ZIP_STR_32B == encoding) {
            input.readerIndex(input.readerIndex() + 1);
            this.length = input.readInt();
            readStr(input, this.length);
        } else if (ZIP_INT_16B == encoding) {
            input.readerIndex(input.readerIndex() + 1);
            this.length = 2;
            setValue(input.readShortLE());
        } else if (ZIP_INT_32B == encoding) {
            input.readerIndex(input.readerIndex() + 1);
            this.length = 4;
            setValue(input.readIntLE());
        } else if (ZIP_INT_64B == encoding) {
            input.readerIndex(input.readerIndex() + 1);
            this.length = 8;
            setValue(input.readLongLE());
        } else if (ZIP_INT_24B == encoding) {
            this.length = 3;
            int val = input.readIntLE();
            this.setValue(val >> 8);
        } else if (ZIP_INT_8B == encoding) {
            input.readerIndex(input.readerIndex() + 1);
            this.length = 1;
            setValue(input.readByte());
        } else {
            int unsigned = input.readUnsignedByte();
            if (unsigned >= ZIP_INT_IMM_MIN && unsigned <= ZIP_INT_IMM_MAX) {
                this.setValue(unsigned - ZIP_INT_IMM_MIN);
            } else {
                throw new ZiplistParseFailException("prev len over big prev len: " + encoding);
            }
        }
    }

    private void readStr(ByteBuf input, int len) {
        byte[] val = new byte[len];
        input.readBytes(val);
        setValue(val);
    }

    private void setValue(long val) {
        this.data = String.valueOf(val).getBytes();
    }

    private void setValue(int val) {
        this.data = String.valueOf(val).getBytes();
    }

    private void setValue(byte[] val) {
        this.data = val;
    }

    @Override
    public String toString() {
        return new String(data);
    }
}
