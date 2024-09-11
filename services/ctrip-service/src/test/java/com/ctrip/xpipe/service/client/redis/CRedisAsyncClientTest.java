package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 6:14 PM
 */
public class CRedisAsyncClientTest {

    @Test
    public void setKV() throws Throwable {

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        AsyncRedisClient client = CRedisAsyncClientFactory.DEFAULT.getOrCreateClient("BBZ_RedisGovUni", null, executorService);
        Object resource = client.select("K");
        CommandFuture<Object> future = client.write(resource, 0, "SET", "K", "K1");
        assertArrayEquals("OK".getBytes(), (byte[]) future.get());
        executorService.shutdown();
    }
}