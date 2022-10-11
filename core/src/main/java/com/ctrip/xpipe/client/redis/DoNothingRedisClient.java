package com.ctrip.xpipe.client.redis;

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
public class DoNothingRedisClient implements AsyncRedisClient {

    private <T> CommandFuture<T> resultFuture(T result) {
        CommandFuture<T> future = new DefaultCommandFuture<>();
        future.setSuccess(result);
        return future;
    }

    @Override
    public Object[] broadcast() {
        return new Object[0];
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
    public CommandFuture<Object> selectDB(int db) {
        return resultFuture("OK");
    }

    @Override
    public CommandFuture<Object> write(Object resource, Object... rawArgs) {
        return resultFuture("OK");
    }

    @Override
    public CommandFuture<Object> multi() {
        return resultFuture("OK");
    }

    @Override
    public CommandFuture<Object> exec(Object... rawArgs) {
        return resultFuture("OK");
    }
}
