package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.redisconf.Callbackable;
import com.lambdaworks.redis.RedisChannelHandler;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnectionStateListener;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * @author marsqing
 *         <p>
 *         Dec 1, 2016 2:28:43 PM
 */
public class RedisSession {

    private static Logger log = LoggerFactory.getLogger(RedisSession.class);

    public static final String KEY_SUBSCRIBE_TIMEOUT_SECONDS = "SUBSCRIBE_TIMEOUT_SECONDS";

    private int waitResultSeconds = 2;

    private int subscribConnsTimeoutSeconds = Integer.parseInt(System.getProperty(KEY_SUBSCRIBE_TIMEOUT_SECONDS, "60"));

    private RedisClient redis;

    private HostPort hostPort;

    private ConcurrentMap<String, PubSubConnectionWrapper> subscribConns = new ConcurrentHashMap<>();

    private AtomicReference<StatefulRedisConnection<String, String>> nonSubscribeConn = new AtomicReference<>();

    private Executor executors;

    private Executor pingAndDelayExecutor;

    public RedisSession(RedisClient redisClient, HostPort hostPort, Executor executors, Executor pingDelayExecutor) {
        this.redis = redisClient;
        redis.addListener(channelListener());
        this.hostPort = hostPort;
        this.executors = executors;
        this.pingAndDelayExecutor = pingDelayExecutor;
    }

    public void check() {

        for (Map.Entry<String, PubSubConnectionWrapper> entry : subscribConns.entrySet()) {

            String channel = entry.getKey();
            PubSubConnectionWrapper pubSubConnectionWrapper = entry.getValue();

            if (System.currentTimeMillis() - pubSubConnectionWrapper.getLastActiveTime() > subscribConnsTimeoutSeconds * 1000) {

                log.info("[check][connectin inactive for a long time, force reconnect]{}, {}", subscribConns, hostPort);
                pubSubConnectionWrapper.closeAndClean();
                subscribConns.remove(channel);

                subscribeIfAbsent(channel, pubSubConnectionWrapper.getCallback());
            }
        }

    }

    public synchronized void closeSubscribedChannel(String channel) {

        PubSubConnectionWrapper pubSubConnectionWrapper = subscribConns.get(channel);
        if (pubSubConnectionWrapper != null) {
            log.info("[closeSubscribedChannel]{}, {}", hostPort, channel);
            pubSubConnectionWrapper.closeAndClean();
            subscribConns.remove(channel);
        }
    }

    public synchronized void subscribeIfAbsent(String channel, SubscribeCallback callback) {

        PubSubConnectionWrapper pubSubConnectionWrapper = subscribConns.get(channel);
        if (pubSubConnectionWrapper != null) {
            pubSubConnectionWrapper.replace(callback);
            return;
        }

        if (!subscribConns.containsKey(channel)) {

            CompletableFuture<StatefulRedisPubSubConnection> pubSubFuture = CompletableFuture
                    .supplyAsync(new Supplier<StatefulRedisPubSubConnection>() {
                @Override
                public StatefulRedisPubSubConnection get() {
                    return redis.connectPubSub();
                }
            }, executors);
            pubSubFuture.whenCompleteAsync((pubSub, th) -> {
                if(th != null) {
                    Throwable exception = th;
                    while(exception instanceof CompletionException) {
                        exception = exception.getCause();
                    }
                    callback.fail(exception);
                    log.warn("Error subscribe to redis {}", hostPort);
                } else {
                    try {
                        pubSub.async().subscribe(channel);
                        PubSubConnectionWrapper wrapper = new PubSubConnectionWrapper(pubSub, callback);

                        pubSub.addListener(new RedisPubSubAdapter<String, String>() {

                            @Override
                            public void message(String channel, String message) {

                                wrapper.setLastActiveTime(System.currentTimeMillis());
                                wrapper.getCallback().message(channel, message);
                            }
                        });
                        subscribConns.put(channel, wrapper);
                    } catch (RuntimeException e) {
                        callback.fail(e);
                        log.warn("Error subscribe to redis {}", hostPort);
                    }
                }
            }, pingAndDelayExecutor);
        }
    }

    private RedisConnectionStateListener channelListener() {

        return new RedisConnectionStateListener() {

            @Override
            public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
                log.debug("[lettuce][onRedisExceptionCaught]{}, {}", hostPort, cause);
            }

            @Override
            public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
                log.debug("[lettuce][onRedisDisconnected]{}, {}", hostPort, connection);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onRedisConnected(RedisChannelHandler<?, ?> connection) {
                log.debug("[lettuce][onRedisConnected]{}, {}", hostPort, connection);
            }
        };
    }

    public synchronized void publish(String channel, String message) {
        asyncExecute(connection -> {
            try {
                connection.async().publish(channel, message);
            } catch (RuntimeException e) {
                // not connected, just ignore
                log.warn("Error publish to redis {}", hostPort);
            }
        }, null);
    }

    public void ping(final PingCallback callback) {
        // if connect has been established
        if(nonSubscribeConn.get() != null) {
            final CompletableFuture<String> future = nonSubscribeConn.get().async().ping().toCompletableFuture();

            future.whenComplete((pong, th) -> {
                if(th != null){
                    callback.fail(th);
                }else{
                    callback.pong(pong);
                }
            });
            return;
        }
        Consumer<StatefulRedisConnection> connectionConsumer = new Consumer<StatefulRedisConnection>() {
            @Override
            public void accept(StatefulRedisConnection statefulRedisConnection) {
                final CompletableFuture<String> future = statefulRedisConnection.async().ping().toCompletableFuture();

                future.whenCompleteAsync((pong, th) -> {
                    if(th != null){
                        callback.fail(th);
                    }else{
                        callback.pong(pong);
                    }
                }, pingAndDelayExecutor);
            }
        };
        Consumer<Throwable> throwableConsumer = new Consumer<Throwable>() {
            @Override
            public void accept(Throwable e) {
                callback.fail(e);
                log.error("[ping]" + hostPort, e);
            }
        };
        asyncExecute(connectionConsumer, throwableConsumer);
    }

    public void role(RollCallback callback) {
        Consumer<StatefulRedisConnection> connectionConsumer = new Consumer<StatefulRedisConnection>() {
            @Override
            public void accept(StatefulRedisConnection statefulRedisConnection) {
                final CompletableFuture<List<Object>> future = statefulRedisConnection.async().role().toCompletableFuture();

                future.whenCompleteAsync((role, th) -> {
                    if (th != null) {
                        callback.fail(th);
                    } else {
                        callback.role((String) role.get(0));
                    }
                }, executors);
            }
        };
        asyncExecute(connectionConsumer, callback::fail);
    }

    public void configRewrite(BiConsumer<String, Throwable> consumer) {
        Consumer<StatefulRedisConnection> connectionConsumer = new Consumer<StatefulRedisConnection>() {
            @Override
            public void accept(StatefulRedisConnection statefulRedisConnection) {
                RedisFuture<String> redisFuture = statefulRedisConnection.async().configRewrite();
                redisFuture.whenCompleteAsync(consumer, executors);
            }
        };
        asyncExecute(connectionConsumer, null);
    }

    public String roleSync() throws InterruptedException, ExecutionException, TimeoutException {

        final CompletableFuture<List<Object>> future = findOrCreateNonSubscribeConnection().async().role().toCompletableFuture();
        return (String) future.get(waitResultSeconds, TimeUnit.SECONDS).get(0);

    }

    public void info(final String infoSection, Callbackable<String> callback) {

        Consumer<StatefulRedisConnection> connectionConsumer = (connection) -> {
            CompletableFuture<String> future = connection.async().info(infoSection).toCompletableFuture();
            future.whenCompleteAsync((info, th) -> {
                if(th != null){
                    log.error("[info]{}", hostPort, th);
                    callback.fail(th);
                }else{
                    callback.success(info);
                }
            }, executors);
        };

        Consumer<Throwable> throwableConsumer = (throwable) -> {
            callback.fail(throwable);
            log.error("[info]{}", hostPort, throwable);
        };

        asyncExecute(connectionConsumer, throwableConsumer);
    }


    public void infoServer(Callbackable<String> callback) {
        String section = "server";
        info(section, callback);
    }

    public void infoReplication(Callbackable<String> callback) {
        String infoReplicationSection = "replication";
        info(infoReplicationSection, callback);
    }

    public void conf(String confSection, Callbackable<List<String>> callback) {
        Consumer<StatefulRedisConnection> connectionConsumer = (connection) -> {
            CompletableFuture<List<String>> future = connection.async().configGet(confSection).toCompletableFuture();
            future.whenCompleteAsync((conf, throwable) -> {
                if(throwable != null) {
                    log.error("[conf]Executing conf command error", throwable);
                    callback.fail(throwable);
                } else {
                    callback.success(conf);
                }
            });
        };

        Consumer<Throwable> throwableConsumer = (throwable) -> {
            callback.fail(throwable);
            log.error("[conf]{}", hostPort, throwable);
        };

        asyncExecute(connectionConsumer, throwableConsumer);
    }

    private void asyncExecute(Consumer<StatefulRedisConnection> connectionConsumer,
                              Consumer<Throwable> throwableConsumer) {
        Supplier<StatefulRedisConnection> supplier = ()->findOrCreateNonSubscribeConnection();
        CompletableFuture.supplyAsync(supplier, executors)
                .whenCompleteAsync((connection, th) -> {
                    if(th != null) {
                        Throwable exception = th;
                        while(exception instanceof CompletionException) {
                            exception = exception.getCause();
                        }
                        log.error("[asyncExecute]" + hostPort, exception);

                        if(throwableConsumer != null) {
                            throwableConsumer.accept(exception);
                        }
                    } else {
                        connectionConsumer.accept(connection);
                    }
                }, executors);
    }

    @Override
    public String toString() {
        return String.format("%s", hostPort.toString());
    }

    private StatefulRedisConnection<String, String> findOrCreateNonSubscribeConnection() {
        if (nonSubscribeConn.get() == null) {
            nonSubscribeConn.set(redis.connect());
        }

        return nonSubscribeConn.get();
    }

    public interface RollCallback {

        void role(String role);

        void fail(Throwable e);
    }

    public interface SubscribeCallback {

        void message(String channel, String message);

        void fail(Throwable e);
    }

    public class PubSubConnectionWrapper {

        private StatefulRedisPubSubConnection<String, String> connection;
        private Long lastActiveTime = System.currentTimeMillis();
        private AtomicReference<SubscribeCallback> callback = new AtomicReference<>();

        public PubSubConnectionWrapper(StatefulRedisPubSubConnection<String, String> connection, SubscribeCallback callback) {
            this.connection = connection;
            this.callback.set(callback);
        }

        public StatefulRedisPubSubConnection<String, String> getConnection() {
            return connection;
        }

        public SubscribeCallback getCallback() {
            return callback.get();
        }

        public void replace(SubscribeCallback callback) {
            this.callback.set(callback);
        }

        public void setLastActiveTime(Long lastActiveTime) {
            this.lastActiveTime = lastActiveTime;
        }

        public Long getLastActiveTime() {
            return lastActiveTime;
        }

        public void closeAndClean() {
            connection.close();
        }
    }

    public void closeConnection() {
        try {
            nonSubscribeConn.get().close();
        } catch (Exception ignore) {}
        for(PubSubConnectionWrapper connectionWrapper : subscribConns.values()) {
            try {
                connectionWrapper.closeAndClean();
            } catch (Exception ignore) {}
        }
    }
}
