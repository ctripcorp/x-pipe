package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Dec 09, 2022 12:02
 */
public class RedisOpSingleKeyTest  {

    @Test
    public void testSize() {

        RedisOpSingleKey op = new RedisOpSingleKey(RedisOpType.SET, string2Bytes("SET K V"), null, null);

        assertEquals(5, op.estimatedSize());
    }

    private byte[][] string2Bytes(String s) {

        String[] ss = s.split(" ");
        int length = ss.length;
        byte[][] b = new byte[length][];

        for (int i = 0; i < length; i++) {
            b[i] = ss[i].getBytes();
        }

        return b;
    }
}