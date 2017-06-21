package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *         <p>
 *         May 11, 2017
 */
public class MetaJacksonTest extends AbstractMetaServerTest{


    @Test
    public void testJackson() throws IOException {

        //for get config from console, use raw ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();

        XpipeMeta rawMeta = getXpipeMeta();

        String metaString = objectMapper.writeValueAsString(rawMeta);

        XpipeMeta metaBack = objectMapper.readValue(metaString, XpipeMeta.class);

        Assert.assertEquals(metaBack, rawMeta);

    }


}
