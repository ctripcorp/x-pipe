package com.ctrip.xpipe.redis.console.health;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import com.lambdaworks.redis.*;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.metric.HostPort;
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

            try {

                StatefulRedisPubSubConnection<String, String> pubSub = redis.connectPubSub();
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
        try {
            findOrCreateNonSubscribeConnection().async().publish(channel, message);
        } catch (RuntimeException e) {
            // not connected, just ignore
            log.warn("Error publish to redis {}", hostPort);
        }
    }

    public void ping(final PingCallback callback) {

        try {
            final CompletableFuture<String> future = findOrCreateNonSubscribeConnection().async().ping().toCompletableFuture();

            future.whenComplete((pong, th) -> {
                if(th != null){
                    callback.fail(th);
                }else{
                    callback.pong(pong);
                }
            });
        } catch (RedisException e) {
            callback.fail(e);
            log.error("[ping]" + hostPort, e);
        }
    }

    public void role(RollCallback callback) {

        final CompletableFuture<List<Object>> future = findOrCreateNonSubscribeConnection().async().role().toCompletableFuture();

        future.whenComplete((role, th) -> {
            if (th != null) {
                callback.fail(th);
            } else {
                callback.role((String) role.get(0));
            }
        });
    }

    public void configRewrite(BiConsumer<String, Throwable> consumer) {

        RedisFuture<String> redisFuture = findOrCreateNonSubscribeConnection().async().configRewrite();
        redisFuture.whenComplete(consumer);

    }

    public String roleSync() throws InterruptedException, ExecutionException, TimeoutException {

        final CompletableFuture<List<Object>> future = findOrCreateNonSubscribeConnection().async().role().toCompletableFuture();
        return (String) future.get(waitResultSeconds, TimeUnit.SECONDS).get(0);

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
