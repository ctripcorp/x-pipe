package com.ctrip.xpipe.client.redis;

import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 2:49 PM
 */
public class DoNothingRedisClientTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {

        AsyncRedisClient client = new DoNothingRedisClient();
        Object shard = client.select("K");
        assertEquals("OK", client.write(shard, 0, Lists.newArrayList("K", "V").toArray()).get());
    }

}