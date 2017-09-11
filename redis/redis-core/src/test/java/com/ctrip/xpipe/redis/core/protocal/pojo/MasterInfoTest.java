package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 11, 2017
 */
public class MasterInfoTest extends AbstractRedisTest{

    @Test
    public void testJson() throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();

        MasterInfo masterInfo = new MasterInfo(randomString(40), 0L);
        masterInfo.setKeeper(true);
        masterInfo.setServerRole(Server.SERVER_ROLE.MASTER);

        String encode = objectMapper.writeValueAsString(masterInfo);

        logger.info("[encode]{}", encode);

        MasterInfo decode = objectMapper.readValue(encode, MasterInfo.class);

        logger.info("[decode]{}", decode);

        Assert.assertEquals(masterInfo.toString(), decode.toString());

    }
}
