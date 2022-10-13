package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 6:14 PM
 */
public class CRedisAsyncClientTest {

    @Test
    public void setKV() throws Throwable {

        AsyncRedisClient client = CRedisAsyncClientFactory.DEFAULT.getOrCreateClient("BBZ_RedisGovUni");
        Object resource = client.select("K");
        CommandFuture<Object> future = client.write(resource, "SET", "K", "K1");
        assertArrayEquals("OK".getBytes(), (byte[]) future.get());
    }
}