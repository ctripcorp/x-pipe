package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class SocketStatsResultTest {

    private static final Logger logger = LoggerFactory.getLogger(SocketStatsResultTest.class);

    private List<String> lines = Lists.newArrayList("hello", "world");

    @Test
    public void testToByteBuf() {
        SocketStatsResult result = new SocketStatsResult(lines);
        ByteBuf byteBuf = result.toByteBuf();
        String str = ByteBufUtils.readToString(byteBuf);
        logger.info("\r\n{}", str);
    }

    @Test
    public void testParseFromArr() {

    }
}