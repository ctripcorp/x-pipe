package com.ctrip.xpipe.redis.keeper.applier.client;

import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:25 PM
 */
public class DoNothingRedisClientTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {

        ApplierRedisClient client = new DoNothingRedisClient();
        Object shard = client.select("K");
        assertEquals("OK", client.write(shard, Lists.newArrayList("K", "V")).get());
    }
}