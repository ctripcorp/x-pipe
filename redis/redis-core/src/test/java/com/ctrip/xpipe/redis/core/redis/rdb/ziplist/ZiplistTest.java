package com.ctrip.xpipe.redis.core.redis.rdb.ziplist;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbStringParser;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2022/6/17
 */
public class ZiplistTest extends AbstractTest {

    private byte[] rdbZiplistBytes = new byte[] {
            0x3e, 0x3e, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x02, 0x6b, 0x31, 0x04, 0x02,
            0x76, 0x31, 0x04, 0x02, 0x6b, 0x32, 0x04, (byte)0xfe, 0x64, 0x03, 0x02, 0x6b, 0x33, 0x04, (byte)0xfe,
            (byte)0x9c, 0x03, 0x02, 0x6b, 0x34, 0x04, (byte)0xfb, 0x02, 0x02, 0x6b, 0x35, 0x04, (byte)0xe0, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x6b, 0x36, 0x04, (byte)0xf0, (byte)0xff, (byte)0xff,
            0x00, (byte)0xff};

    @Test
    public void testZiplistDecode() {
        RdbStringParser stringParser = new RdbStringParser(new DefaultRdbParseContext());
        byte[] ziplistBytes = stringParser.read(Unpooled.wrappedBuffer(rdbZiplistBytes));

        Ziplist ziplist = new Ziplist(ziplistBytes);
        List<ZiplistEntry> entries = ziplist.getEntries();
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < entries.size(); i += 2) {
            map.put(entries.get(i).toString(), entries.get(i + 1).toString());
        }

        Assert.assertEquals("v1", map.get("k1"));
        Assert.assertEquals("100", map.get("k2"));
        Assert.assertEquals("-100", map.get("k3"));
        Assert.assertEquals("10", map.get("k4"));
        Assert.assertEquals("4294967296", map.get("k5"));
        Assert.assertEquals("65535", map.get("k6"));
    }

}
