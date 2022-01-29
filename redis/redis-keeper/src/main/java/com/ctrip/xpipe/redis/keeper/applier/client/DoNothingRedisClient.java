package com.ctrip.xpipe.redis.keeper.applier.client;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:20 PM
 */
public class DoNothingRedisClient implements ApplierRedisClient {

    private <T> CommandFuture<T> resultFuture(T result) {
        CommandFuture<T> future = new DefaultCommandFuture<>();
        future.setSuccess(result);
        return future;
    }

    @Override
    public CommandFuture<Boolean> set(byte[] key, byte[] value) {
        return resultFuture(true);
    }

    @Override
    public CommandFuture<Boolean> delete(byte[] key) {
        return resultFuture(true);
    }
}
