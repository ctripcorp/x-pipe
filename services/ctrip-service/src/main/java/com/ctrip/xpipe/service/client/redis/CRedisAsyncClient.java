package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
import credis.java.client.AsyncCacheProvider;
import credis.java.client.async.command.CRedisAsyncRequest;
import credis.java.client.async.impl.AsyncCacheProviderImpl;
import credis.java.client.async.qclient.CRedisSessionLocator;
import credis.java.client.async.qclient.network.CRedisSessionChannel;
import qunar.tc.qclient.redis.codec.Codec;
import qunar.tc.qclient.redis.codec.SedisCodec;
import qunar.tc.qclient.redis.command.value.ValueResult;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 2:37 PM
 */
public class CRedisAsyncClient implements AsyncRedisClient {

    final AsyncCacheProvider provider;

    final Codec codec;

    public CRedisAsyncClient(AsyncCacheProvider provider) {
        this.provider = provider;
        this.codec = new SedisCodec();
    }

    @Override
    public Object[] resources() {
        /* not efficient */
        return locator().getAllSession().values().toArray();
    }

    @Override
    public Object select(Object key) {
        return locator().getSessionForObject(key, true);
    }

    @Override
    public Map<Object, List<Object>> selectMulti(List<Object> keys) {
        return new CRedisSessionChannelMapWrapper(locator().getSessionForList(true, keys.toArray()));
    }

    @Override
    public CommandFuture<Object> write(Object resource, Object... rawArgs) {
        CRedisSessionChannel channel = (CRedisSessionChannel) resource;
        GenericCommand command = new GenericCommand(codec);
        for (Object rawArg : rawArgs) {
            command.write(rawArg);
        }
        CRedisAsyncRequest<Object> request = CRedisAsyncRequest.from(new ValueResult<>(), 0 /* this should be ignored */);
        DefaultCommandFuture<Object> commandFuture = new DefaultCommandFuture<>();
        channel.dispatch(request, command).addListener(new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {
                commandFuture.setSuccess(result);
            }

            @Override
            public void onFailure(Throwable t) {
                commandFuture.setFailure(t);
            }
        }, 1, TimeUnit.SECONDS, MoreExecutors.directExecutor());
        return commandFuture;
    }

    private CRedisSessionLocator locator() {
        return ((AsyncCacheProviderImpl) provider).asyncClient.cRedisSessionLocator;
    }
}
