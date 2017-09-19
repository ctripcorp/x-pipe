package com.ctrip.xpipe.redis.console.health;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.endpoint.HostPort;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;


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

    private final static int THREAD_NUMBER = 5;

    private static Executor executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("RedisSession"));


    public RedisSession(RedisClient redisClient, HostPort hostPort) {
        this.redis = redisClient;
        redis.addListener(channelListener());
        this.hostPort = hostPort;
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
                    callback.fail(new Exception(th));
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
            }, executors);
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
                }, executors);
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
        asyncExecute(connectionConsumer, null);
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

    private void asyncExecute(Consumer<StatefulRedisConnection> connectionConsumer,
                              Consumer<Throwable> throwableConsumer) {
        Supplier<StatefulRedisConnection> supplier = new Supplier<StatefulRedisConnection>() {
            @Override
            public StatefulRedisConnection get() {
                return findOrCreateNonSubscribeConnection();
            }
        };
        CompletableFuture.supplyAsync(supplier, executors)
                .whenCompleteAsync((connection, th) -> {
                    if(th != null) {
                        log.error("[asyncExecute]" + hostPort, th);
                        if(throwableConsumer != null)
                            throwableConsumer.accept(th);
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

        void fail(Exception e);
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
}
