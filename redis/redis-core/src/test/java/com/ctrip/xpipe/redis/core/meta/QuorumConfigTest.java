package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public class QuorumConfigTest extends AbstractRedisTest{

    @Test
    public void testQuorumConfig(){

        QuorumConfig config1 = new QuorumConfig(5, 3);
        QuorumConfig config2 = new QuorumConfig(5, 3);
        QuorumConfig config3 = new QuorumConfig(5, 4);

        Assert.assertEquals(config1, config2);
        Assert.assertNotEquals(config1, config3);

    }

    @Test
    public void testJson(){

        QuorumConfig config1 = new QuorumConfig(5, 3);

        logger.info("{}", JsonCodec.DEFAULT.encode(config1));

        String result = "{\"total\":5,\"quorum\":3, \"other\": 4}";

        QuorumConfig decode = JsonCodec.DEFAULT.decode(result, QuorumConfig.class);

        logger.info("decode: {}", decode);

        decode = JsonCodec.DEFAULT.decode("{}", QuorumConfig.class);
        logger.info("decode: {}", decode);
    }

}
