package com.ctrip.xpipe.redis.keeper.hetero.manual;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.junit.Test;

/**
 * @author Slight
 * <p>
 * Dec 13, 2022 09:57
 */
public class SendCommandTest extends AbstractRedisTest {

    @Test
    public void sendRandom() {

        RedisMeta redis = new RedisMeta();
        redis.setIp("127.0.0.1");
        redis.setPort(6379);

        while(true) {
            sendRandomMessage(redis, 10000);
            sleep(1000);
        }
    }
}
