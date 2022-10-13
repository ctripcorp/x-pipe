package com.ctrip.xpipe.redis.core.util;

/**
 * @author lishanglin
 * date 2022/6/4
 */
public class RedisLzfUtil {

    private RedisLzfUtil() {

    }

    public static int decode(byte[] input, byte[] output) {
        return decode(input, 0, input.length, output);
    }

    // refer to lzf_decompress in Redis
    public static int decode(byte[] input, int src, int inputLength, byte[] output) {
        int ip = src;
        int ipEnd = src + inputLength;
        int op = 0;

        while (ip < ipEnd) {
            short ctrl = (short) (input[ip++] & 0xff);
            if (ctrl < 32) {
                ctrl++;
                System.arraycopy(input, ip, output, op, ctrl);
                ip += ctrl;
                op += ctrl;
            } else {
                int len = ctrl >> 5;
                int ref = op - ((ctrl & 0x1f) << 8) - 1;
                if (7 == len) {
                    len += (short) (input[ip++] & 0xff);
                }
                ref -= (short) (input[ip++] & 0xff);
                len += 2;
                if (op >= ref + len) {
                    System.arraycopy(output, ref, output, op, len);
                    op += len;
                } else {
                    do {
                        output[op++] = output[ref++];
                    } while (--len > 0);
                }

            }
        }

        return op;
    }

}
