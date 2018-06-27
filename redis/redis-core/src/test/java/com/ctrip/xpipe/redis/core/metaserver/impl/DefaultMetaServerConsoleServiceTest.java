package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 14, 2017
 */
public class DefaultMetaServerConsoleServiceTest extends AbstractRedisTest{

    @Test
    public void test(){

        try {
            logger.info("[begin]");
            MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService("http://10.3.2.39:1234");
            consoleService.doChangePrimaryDc("cluster1", "shard-0", "oy", null);
        }catch (Exception e){
            logger.error("[Exception]", e);
        }

    }
}
