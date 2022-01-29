package com.ctrip.xpipe.redis.keeper.applier.client;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:25 PM
 */
public class DoNothingRedisClientTest {

    @Test
    public void set() throws ExecutionException, InterruptedException {

        ApplierRedisClient client = new DoNothingRedisClient();
        assertTrue(client.set(new byte[]{}, new byte[]{}).get());
    }

    @Test
    public void delete() throws ExecutionException, InterruptedException {

        ApplierRedisClient client = new DoNothingRedisClient();
        assertTrue(client.delete(new byte[]{}).get());
    }
}