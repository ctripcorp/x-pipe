package com.ctrip.xpipe.redis.keeper.applier.client;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public Object select(Object key) {
        return new Object();
    }

    @Override
    public Map<Object, List<Object>> selectMulti(List<Object> keys) {
        return new HashMap<>();
    }

    @Override
    public CommandFuture<Object> write(Object shard, List<Object> rawArgs) {
        //in most cases, return OK
        return resultFuture("OK");
    }
}
