package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class RedisInstanceInfoTest extends AbstractConsoleTest {

    @Test
    public void testJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        String body = "{\"clusterId\":\"test_dbatools_xpipe_v20221117\",\"activeDc\":\"SHARB\",\"clusterType\":\"SINGLE_DC\",\"redisCheckRules\":[],\"dcGroupType\":null,\"dcId\":\"SHARB\",\"shardId\":\"test_dbatools_xpipe_v20221117_1\",\"hostPort\":{\"port\":6379,\"host\":\"127.0.0.1\"},\"crossRegion\":false,\"shardDbId\":51219,\"master\":false,\"activeDcAllShardIds\":[51219],\"heteroCluster\":false}";
        try {
            DefaultRedisInstanceInfo info = objectMapper.readValue(body, DefaultRedisInstanceInfo.class);
            logger.info("{}", info.toString());
        } catch (Throwable e) {
            Assert.fail();
        }
    }

}
