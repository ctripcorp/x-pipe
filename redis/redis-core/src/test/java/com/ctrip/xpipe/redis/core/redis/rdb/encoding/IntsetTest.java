package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbStringParser;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2022/6/17
 */
public class IntsetTest extends AbstractTest {

    private byte[] rdbIntsetBytes = new byte[] {0x14, 0x02, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, (byte)0xfd,
            (byte)0xff, (byte)0xfe, (byte)0xff, (byte)0xff, (byte)0xff, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00};

    @Test
    public void testIntsetDecode() {
        RdbStringParser rdbStringParser = new RdbStringParser(new DefaultRdbParseContext());
        byte[] rawData = rdbStringParser.read(Unpooled.wrappedBuffer(rdbIntsetBytes));

        Intset intset = new Intset(rawData);
        Assert.assertEquals(6, intset.size());
        Assert.assertEquals(Intset.ENC_TYPE.INTSET_ENC_INT16, intset.getEncoding());
        Assert.assertEquals(Arrays.asList((short) -3, (short) -2, (short) -1, (short) 1, (short) 2, (short) 3), intset.getData());
    }

}
