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

        MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService("http://localhost:9747");
        consoleService.doChangePrimaryDc("cluster1", "shard-0", "oy", null);

    }
}
