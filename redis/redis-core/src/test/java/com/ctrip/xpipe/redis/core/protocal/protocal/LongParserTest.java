package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Apr 09, 2018
 */
public class LongParserTest {

    private LongParser longParser = new LongParser();

    @Test
    public void read() throws Exception {
        System.out.println("");
        RedisClientProtocol<Long> protocol =  longParser.read(Unpooled.copiedBuffer(":1\r\n".getBytes()));
        Long payload = protocol.getPayload();
        System.out.println(payload);
    }

}