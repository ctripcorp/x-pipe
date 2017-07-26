package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 26, 2017
 */
public class MasterRoleTest extends AbstractRedisTest{

    @Test
    public void testFormat(){

        ByteBuf format = new MasterRole().format();

        String formatStr = ByteBufUtils.readToString(format);

        Assert.assertEquals("*3\r\n+master\r\n:0\r\n*0\r\n", formatStr);

        logger.info("{}", formatStr);


    }
}
