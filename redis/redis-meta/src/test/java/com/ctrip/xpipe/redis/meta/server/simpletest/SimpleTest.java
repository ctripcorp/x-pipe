package com.ctrip.xpipe.redis.meta.server.simpletest;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class SimpleTest extends AbstractMetaServerTest{

    @Test
    public void testPort(){

        logger.info("{}", isUsable(10033));

    }
}
