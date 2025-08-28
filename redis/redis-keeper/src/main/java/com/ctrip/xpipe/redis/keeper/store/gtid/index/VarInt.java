package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import java.io.IOException;
import java.nio.ByteBuffer;

public class VarInt {

    public static int varIntSize(int i) {
        int result = 0;
        do {
            result++;
            i >>>= 7;
        } while (i != 0);
        return result;
    }

    public static int getVarInt(ByteBuffer src) {
        int tmp;
        if ((tmp = src.get()) >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = src.get()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = src.get()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = src.get()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = src.get()) << 28;
                    while (tmp < 0) {
                        // We get into this loop only in the case of overflow.
                        // By doing this, we can call getVarInt() instead of
                        // getVarLong() when we only need an int.
                        tmp = src.get();
                    }
                }
            }
        }
        return result;
    }

    public static byte[] encode(int value) {
        byte[] bytes = new byte[varIntSize(value)];
        putVarInt(value, bytes, 0);
        return bytes;
    }

    public static ByteBuffer encodeToByteBuffer(int value) {
        byte[] bytes = encode(value);
        return ByteBuffer.wrap(bytes);
    }


    public static int putVarInt(int v, byte[] sink, int offset) {
        do {
            // Encode next 7 bits + terminator bit
            int bits = v & 0x7F;
            v >>>= 7;
            byte b = (byte) (bits + ((v != 0) ? 0x80 : 0));
            sink[offset++] = b;
        } while (v != 0);
        return offset;
    }


    public static int decodeArray(ByteBuffer bytes, int offset) throws IOException {
        int sum = 0;
        for(int i = 0; i <= offset; i++) {
            int tmp = getVarInt(bytes);
            sum += tmp;
        }
        return sum;
    }

}
