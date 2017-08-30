package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 29, 2017
 */
public class DefaultMetaServersTest extends AbstractMetaServerTest{

    private DefaultMetaServers metaServers;

    @Before
    public void beforeDefaultMetaServersTest(){
        metaServers = new DefaultMetaServers();
    }

    @Test
    public void testGetServerIdFromPath(){

        Assert.assertEquals("1", metaServers.getServerIdFromPath("/servers/1", "/servers"));

    }
}
