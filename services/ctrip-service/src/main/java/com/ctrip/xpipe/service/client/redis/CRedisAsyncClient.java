package com.ctrip.xpipe.service.client.redis;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.google.common.util.concurrent.FutureCallback;
import credis.java.client.AsyncCacheProvider;
import credis.java.client.async.command.CRedisAsyncRequest;
import credis.java.client.async.impl.AsyncCacheProviderImpl;
import credis.java.client.async.qclient.CRedisClusterSessionLocator;
import credis.java.client.async.qclient.CRedisSessionLocator;
import credis.java.client.async.qclient.network.CRedisSessionChannel;
import credis.java.client.config.ConfigFrozenAware;
import credis.java.client.config.route.ConfigFrozenRoute;
import credis.java.client.sync.RedisClient;
import credis.java.client.sync.applier.ApplierCacheProvider;
import credis.java.client.transaction.RedisTransactionClient;
import qunar.tc.qclient.redis.codec.Codec;
import qunar.tc.qclient.redis.codec.SedisCodec;
import qunar.tc.qclient.redis.command.value.ValueResult;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 2:37 PM
 */
public class CRedisAsyncClient implements AsyncRedisClient {

    final AsyncCacheProviderImpl asyncProvider;

    final Codec codec;

    final ApplierCacheProvider txnProvider;

    final ExecutorService credisNotifyThread;

    final ConfigFrozenRoute configFrozenRoute;

    boolean isInMulti = false;

    int db = 0;

    // simple fix locator parallel
    private final Object locatorLock = new Object();

    public CRedisAsyncClient(AsyncCacheProvider asyncProvider, ApplierCacheProvider txnProvider, ExecutorService credisNotifyExecutor, ConfigFrozenRoute configFrozenRoute) {
        this.asyncProvider = (AsyncCacheProviderImpl) asyncProvider;
        this.txnProvider = txnProvider;
        this.configFrozenRoute = configFrozenRoute;
        this.codec = new SedisCodec();
        this.credisNotifyThread = credisNotifyExecutor;
    }

    @Override
    public Object[] broadcast() {
        /* not efficient */
        synchronized (locatorLock) {
            return locator().getAllSession(true).toArray();
        }
    }

    public Map<RedisClient, RedisTransactionClient> clients2TxnClients = new HashMap<>();

    @Override
    public Object select(Object key) {
        if (isInMulti) {
            RedisClient client = txnProvider.getClientOnlyForApplier(key, clients2TxnClients.keySet());
            if (!client.isInMulti()) {
                RedisTransactionClient txnClient = client.multi();
                clients2TxnClients.put(client, txnClient);
            }
            return client;
        }
        synchronized (locatorLock) {
            return locator().getSessionForObject(key, true);
        }
    }

    @Override
    public Map<Object, List<Object>> selectMulti(List<Object> keys) {
        if (isInMulti) {
            Map<RedisClient, List<Object>> clients = txnProvider.getClientsOnlyForApplier(keys, clients2TxnClients.keySet());
            for (RedisClient client : clients.keySet()) {
                if (!client.isInMulti()) {
                    RedisTransactionClient txnClient = client.multi();
                    clients2TxnClients.put(client, txnClient);
                }
            }
            return new TMapWrapper<>(clients);
        }
        synchronized (locatorLock) {
            return new TMapWrapper<>(locator().getSessionForList(true, keys.toArray()));
        }
    }

    @Override
    public void selectDB(int db) {
        this.db = db;
    }

    private void cleanTransactionResource() {
        if (clients2TxnClients.isEmpty()) return;

        for (Map.Entry<RedisClient, RedisTransactionClient> redisClientRedisTransactionClientEntry : clients2TxnClients.entrySet()) {
            RedisTransactionClient redisTransactionClient = redisClientRedisTransactionClientEntry.getValue();
            try {
                // if transaction still not finish, discard
                redisTransactionClient.discard();
            } catch (Throwable t) {
                // ignore
            }
            RedisClient redisClient = redisClientRedisTransactionClientEntry.getKey();
            try {
                // release connection resource
                redisClient.destroy();
            } catch (Throwable t) {
                // ignore, connection will recreate by connection pool
            }
        }
        clients2TxnClients.clear();
    }

    private void resetTransactionState() {
        cleanTransactionResource();
        isInMulti = false;
    }

    @Override
    public CommandFuture<Object> write(Object resource, Object... rawArgs) {
        if (isInMulti) {
            try {
                RedisClient client = (RedisClient) resource;
                client.selectDB(db);
                RedisTransactionClient txnClient = clients2TxnClients.get(client);
                txnClient.write(rawArgs);
                return resultFuture("OK");
            } catch (Throwable t) {
                resetTransactionState();
                return errorFuture(t);
            }
        }
        CRedisSessionChannel channel = (CRedisSessionChannel) resource;
        GenericCommand command = new GenericCommand(codec);
        for (Object rawArg : rawArgs) {
            command.write(rawArg);
        }
        CRedisAsyncRequest<Object> request = CRedisAsyncRequest.from(new ValueResult<>(), db);
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
        }, 1, TimeUnit.SECONDS, credisNotifyThread);
        return commandFuture;
    }

    @Override
    public CommandFuture<Object> multi() {
        if (isInMulti) {
            return errorFuture("multi() is called when client is in multi");
        }
        isInMulti = true;
        return resultFuture("OK");
    }

    @Override
    public CommandFuture<Object> exec(Object... rawArgs) {
        if (txnProvider.isValidOnlyForApplier(clients2TxnClients.keySet())) {
            try {
                List<Object> allResults = new LinkedList<>();
                for (RedisTransactionClient txnClient : clients2TxnClients.values()) {
                    List<Object> results = txnClient.exec(rawArgs);
                    allResults.addAll(results);
                }
                // if exception is GTID executed Exception, guarantee all exec command executed
                for (Object result : allResults) {
                    if (result instanceof Throwable) {
                        return errorFuture((Throwable) result);
                    }
                }
                return resultFuture("OK");
            } catch (Throwable t) {
                return errorFuture(t);
            } finally {
                resetTransactionState();
            }
        } else {
            isInMulti = false;
            return errorFuture("txnClients not valid when exec() called");
        }
    }

    @Override
    public void freezeConfig() {
        configFrozenRoute.startFreeze();
    }

    @Override
    public void stopFreezeConfig() {
        configFrozenRoute.stopFreeze();
    }

    @Override
    public long getFreezeLastMillis() {
        return configFrozenRoute.getFrozenLastMillis(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        ((CRedisClusterSessionLocator) locator()).destroy();
        configFrozenRoute.destroy();
    }

    private CRedisSessionLocator locator() {
        return asyncProvider.asyncClient.cRedisSessionLocator;
    }

    private <T> CommandFuture<T> resultFuture(T result) {
        CommandFuture<T> future = new DefaultCommandFuture<>();
        future.setSuccess(result);
        return future;
    }

    private CommandFuture<Object> errorFuture(Throwable cause) {
        CommandFuture<Object> future = new DefaultCommandFuture<>();
        future.setFailure(cause);
        return future;
    }

    private CommandFuture<Object> errorFuture(String message) {
        CommandFuture<Object> future = new DefaultCommandFuture<>();
        future.setFailure(new XpipeRuntimeException(message));
        return future;
    }

    private CommandFuture<Object> errorFuture(String message, Throwable cause) {
        CommandFuture<Object> future = new DefaultCommandFuture<>();
        future.setFailure(new XpipeRuntimeException(message, cause));
        return future;
    }
}
