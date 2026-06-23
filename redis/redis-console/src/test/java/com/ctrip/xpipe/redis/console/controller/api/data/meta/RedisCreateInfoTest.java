package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.tuple.Pair;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class RedisCreateInfoTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testDeserializeListWithRedises() throws Exception {
        String json = "[{\"dcId\": \"SHAXY\", \"redises\": \"10.110.40.44:6379,10.112.62.222:6379\"},"
                + "{\"dcId\": \"SHARB\", \"redises\": \"10.56.139.209:6379,10.98.225.222:6379\"}]";

        List<RedisCreateInfo> infos = objectMapper.readValue(json, new TypeReference<List<RedisCreateInfo>>() {});

        Assert.assertEquals(2, infos.size());
        Assert.assertEquals("SHAXY", infos.get(0).getDcId());
        Assert.assertEquals("10.110.40.44:6379,10.112.62.222:6379", infos.get(0).getRedises());
        Assert.assertEquals("SHARB", infos.get(1).getDcId());
        Assert.assertEquals("10.56.139.209:6379,10.98.225.222:6379", infos.get(1).getRedises());

        Map<Pair<String, Integer>, String> addrToAzName = infos.get(0).getAddrToAzName();
        Assert.assertEquals(2, addrToAzName.size());
        Assert.assertTrue(addrToAzName.containsKey(new Pair<>("10.110.40.44", 6379)));
        Assert.assertTrue(addrToAzName.containsKey(new Pair<>("10.112.62.222", 6379)));
    }

    @Test
    public void testDeserializeWithRedisesWithAz() throws Exception {
        String json = "{\"dcId\": \"SHAXY\", \"clusterId\": \"cluster1\", \"shardName\": \"shard1\","
                + "\"redisesWithAz\": [{\"addr\": \"10.110.40.44:6379\", \"azName\": \"az1\"},"
                + "{\"addr\": \"10.112.62.222:6379\", \"azName\": \"az2\"}]}";

        RedisCreateInfo info = objectMapper.readValue(json, RedisCreateInfo.class);

        Assert.assertEquals("SHAXY", info.getDcId());
        Assert.assertEquals("cluster1", info.getClusterId());
        Assert.assertEquals("shard1", info.getShardName());
        Assert.assertEquals(2, info.getRedisesWithAz().size());
        Assert.assertEquals("az1", info.getRedisesWithAz().get(0).getAzName());

        Map<Pair<String, Integer>, String> addrToAzName = info.getAddrToAzName();
        Assert.assertEquals("az1", addrToAzName.get(new Pair<>("10.110.40.44", 6379)));
        Assert.assertEquals("az2", addrToAzName.get(new Pair<>("10.112.62.222", 6379)));
    }

    @Test
    public void testSerializeDoesNotExposeComputedProperties() throws Exception {
        RedisCreateInfo info = new RedisCreateInfo()
                .setDcId("SHAXY")
                .setRedises("10.110.40.44:6379")
                .setRedisesWithAz(Lists.newArrayList(
                        new RedisWithAzInfo().setAddr("10.110.40.44:6379").setAzName("az1")));

        String json = objectMapper.writeValueAsString(info);

        Assert.assertFalse(json.contains("addrToAzName"));
        Assert.assertFalse(json.contains("redisAddresses"));
    }
}
