package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.health.redisconf.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.PublishCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeListener;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;


/**
 * @author marsqing
 *         <p>
 *         Dec 1, 2016 2:28:43 PM
 */
public class RedisSession {

    private static Logger logger = LoggerFactory.getLogger(RedisSession.class);

    public static final String KEY_SUBSCRIBE_TIMEOUT_SECONDS = "SUBSCRIBE_TIMEOUT_SECONDS";

    private int waitResultSeconds = 2;

    private int subscribConnsTimeoutSeconds = Integer.parseInt(System.getProperty(KEY_SUBSCRIBE_TIMEOUT_SECONDS, "60"));

    private Endpoint endpoint;

    private ConcurrentMap<String, PubSubConnectionWrapper> subscribConns = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduled;

    private SimpleObjectPool<NettyClient> requestResponseCommandPool;

    private SimpleObjectPool<NettyClient> subscribePool;

    public RedisSession(Endpoint endpoint, ScheduledExecutorService scheduled,
                        XpipeNettyClientKeyedObjectPool requestResponseNettyClientPool,
                        XpipeNettyClientKeyedObjectPool subscribeNettyClientPool) {
        this.endpoint = endpoint;
        this.scheduled = scheduled;
        this.requestResponseCommandPool = requestResponseNettyClientPool.getKeyPool(endpoint);
        this.subscribePool = subscribeNettyClientPool.getKeyPool(endpoint);
    }

    public void check() {

        for (Map.Entry<String, PubSubConnectionWrapper> entry : subscribConns.entrySet()) {

            String channel = entry.getKey();
            PubSubConnectionWrapper pubSubConnectionWrapper = entry.getValue();

            if (System.currentTimeMillis() - pubSubConnectionWrapper.getLastActiveTime() > subscribConnsTimeoutSeconds * 1000) {

                logger.info("[check][connectin inactive for a long time, force reconnect]{}, {}", subscribConns, endpoint);
                pubSubConnectionWrapper.closeAndClean();
                subscribConns.remove(channel);

                subscribeIfAbsent(channel, pubSubConnectionWrapper.getCallback());
            }
        }

    }

    public synchronized void closeSubscribedChannel(String channel) {

        PubSubConnectionWrapper pubSubConnectionWrapper = subscribConns.get(channel);
        if (pubSubConnectionWrapper != null) {
            logger.info("[closeSubscribedChannel]{}, {}", endpoint, channel);
            pubSubConnectionWrapper.closeAndClean();
            subscribConns.remove(channel);
        }
    }

    public synchronized void subscribeIfAbsent(String channel, SubscribeCallback callback) {

        PubSubConnectionWrapper pubSubConnectionWrapper = subscribConns.get(channel);
        if (pubSubConnectionWrapper == null || pubSubConnectionWrapper.shouldCreateNewSession()) {
            if(pubSubConnectionWrapper != null) {
                pubSubConnectionWrapper.closeAndClean();
            }
            SubscribeCommand command = new SubscribeCommand(subscribePool, scheduled, channel);
            command.future().addListener(new CommandFutureListener<Object>() {
                @Override
                public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                    subscribConns.remove(channel);
                }
            });
            PubSubConnectionWrapper wrapper = new PubSubConnectionWrapper(command, callback);
            subscribConns.put(channel, wrapper);
        } else {
            pubSubConnectionWrapper.replace(callback);
        }

    }

    public synchronized void publish(String channel, String message) {
        PublishCommand pubCommand = new PublishCommand(requestResponseCommandPool, scheduled, channel, message);
        if(endpoint instanceof ProxyEnabled) {
            pubCommand.setCommandTimeoutMilli(5000);
        }
        pubCommand.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()) {
                    logger.warn("Error publish to redis {}", endpoint);
                }
            }
        });
    }

    public void ping(final PingCallback callback) {
        PingCommand pingCommand = new PingCommand(requestResponseCommandPool, scheduled);
        pingCommand.execute().addListener(new CommandFutureListener<String>() {
            @Override
            public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                if(commandFuture.isSuccess()) {
                    callback.pong(commandFuture.get());
                } else {
                    callback.fail(commandFuture.cause());
                }
            }
        });
    }

    public void role(RollCallback callback) {
        new RoleCommand(requestResponseCommandPool, scheduled).execute().addListener(new CommandFutureListener<Role>() {
            @Override
            public void operationComplete(CommandFuture<Role> commandFuture) throws Exception {
                if(commandFuture.isSuccess()) {
                    callback.role(commandFuture.get().getServerRole().name());
                } else {
                    callback.fail(commandFuture.cause());
                }
            }
        });
    }

    public void configRewrite(BiConsumer<String, Throwable> consumer) {
        new ConfigRewrite(requestResponseCommandPool, scheduled).execute().addListener(new CommandFutureListener<String>() {
            @Override
            public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                if(commandFuture.isSuccess()) {
                    consumer.accept(commandFuture.get(), null);
                } else {
                    consumer.accept(null, commandFuture.cause());
                }
            }
        });
    }

    public String roleSync() throws InterruptedException, ExecutionException, TimeoutException {

        return new RoleCommand(requestResponseCommandPool, waitResultSeconds * 1000, true, scheduled).execute().get().getServerRole().name();

    }

    public void info(final String infoSection, Callbackable<String> callback) {
        new InfoCommand(requestResponseCommandPool, infoSection, scheduled).execute()
                .addListener(new CommandFutureListener<String>() {
                    @Override
                    public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                        if(!commandFuture.isSuccess()) {
                            callback.fail(commandFuture.cause());
                        } else {
                            callback.success(commandFuture.get());
                        }
                    }
                });
    }


    public void infoServer(Callbackable<String> callback) {
        String section = "server";
        info(section, callback);
    }

    public void infoReplication(Callbackable<String> callback) {
        String infoReplicationSection = "replication";
        info(infoReplicationSection, callback);
    }

    public void isDiskLessSync(Callbackable<Boolean> callback) {
        new ConfigGetCommand.ConfigGetDisklessSync(requestResponseCommandPool, scheduled)
                .execute().addListener(new CommandFutureListener<Boolean>() {
            @Override
            public void operationComplete(CommandFuture<Boolean> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()) {
                    callback.fail(commandFuture.cause());
                } else {
                    callback.success(commandFuture.get());
                }
            }
        });
    }

    @Override
    public String toString() {
        return String.format("%s", endpoint.toString());
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

        private Long lastActiveTime = System.currentTimeMillis();
        private AtomicReference<SubscribeCallback> callback = new AtomicReference<>();
        private AtomicReference<CommandFuture> subscribeCommandFuture = new AtomicReference<>();
        private AtomicReference<SubscribeCommand> command = new AtomicReference<>();


        public PubSubConnectionWrapper(SubscribeCommand command, SubscribeCallback callback) {
            this.command.set(command);
            this.callback.set(callback);
            command.addChannelListener(new SubscribeListener() {
                @Override
                public void message(String channel, String message) {
                    setLastActiveTime(System.currentTimeMillis());
                    getCallback().message(channel, message);
                }
            });

            command.future().addListener(new CommandFutureListener<Object>() {
                @Override
                public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                    if(!commandFuture.isSuccess()) {
                        getCallback().fail(commandFuture.cause());
                    }
                }
            });
            CommandFuture commandFuture = command.execute();
            this.subscribeCommandFuture.set(commandFuture);
        }

        public void closeAndClean() {
            command.get().unSubscribe();
            if(!getSubscribeCommandFuture().isDone()) {
                subscribeCommandFuture.get().cancel(true);
            }
        }

        public CommandFuture getSubscribeCommandFuture() {
            return subscribeCommandFuture.get();
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

        public boolean shouldCreateNewSession() {
            return getSubscribeCommandFuture().isDone();
        }
    }

    public void closeConnection() {
        for(PubSubConnectionWrapper connectionWrapper : subscribConns.values()) {
            try {
                connectionWrapper.closeAndClean();
            } catch (Exception ignore) {}
        }
    }

    public RedisSession setKeyedNettyClientPool(XpipeNettyClientKeyedObjectPool keyedNettyClientPool) {
        this.requestResponseCommandPool = keyedNettyClientPool.getKeyPool(endpoint);
        return this;
    }
}
