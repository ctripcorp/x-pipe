package com.ctrip.xpipe.redis.console.health;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.metric.HostPort;
import com.lambdaworks.redis.RedisChannelHandler;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnectionStateListener;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 2:28:43 PM
 */
public class RedisSession {

	private static Logger log = LoggerFactory.getLogger(RedisSession.class);

	private int waitResultSeconds = 2;

	private RedisClient redis;

	private HostPort hostPort;

	private ConcurrentMap<String, StatefulRedisPubSubConnection<String, String>> subscribConns = new ConcurrentHashMap<>();

	private AtomicReference<StatefulRedisConnection<String, String>> nonSubscribeConn = new AtomicReference<>();

	public RedisSession(RedisClient redisClient, HostPort hostPort) {
		this.redis = redisClient;
		this.hostPort = hostPort;
	}

	public synchronized void subscribeIfAbsent(String channel, SubscribeCallback callback) {

		RedisConnectionStateListener connectionListener = channelListener(channel);

		if (!subscribConns.containsKey(channel)) {

			try {
				redis.addListener(connectionListener);
				StatefulRedisPubSubConnection<String, String> pubSub = redis.connectPubSub();
				pubSub.addListener(new RedisPubSubAdapter<String, String>() {

					@Override
					public void message(String channel, String message) {
						callback.message(channel, message);
					}
				});
				subscribConns.put(channel, pubSub);
			} catch (RuntimeException e) {
				callback.fail(e);
				log.warn("Error subscribe to redis {}", hostPort);
				redis.removeListener(connectionListener);
			}
		}
	}

	private RedisConnectionStateListener channelListener(String channel) {

		return new RedisConnectionStateListener() {

			@Override
			public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
				log.info("[onRedisExceptionCaught]{}, {}", hostPort, cause);
			}

			@Override
			public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
				log.info("[onRedisDisconnected]{}, {}", hostPort, connection);
			}

			@SuppressWarnings("unchecked")
			@Override
			public void onRedisConnected(RedisChannelHandler<?, ?> connection) {
				log.info("[onRedisConnected]{}, {}", hostPort, connection);
				if(connection instanceof StatefulRedisPubSubConnection){
					log.info("[onRedisConnected][subscribe]{},{}", hostPort, channel);
					StatefulRedisPubSubConnection<String, String>  pubsubConnection = (StatefulRedisPubSubConnection<String, String>)connection;
					pubsubConnection.async().subscribe(channel);
				}

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

		try{
			final CompletableFuture<String> future = findOrCreateNonSubscribeConnection().async().ping().toCompletableFuture();
			future.thenRun(new Runnable() {

				@Override
				public void run() {
					if (future.isDone()) {
						callback.pong(future.getNow(null));
					} else {
						callback.fail(new Exception(" future not done " + future));
					}
				}
			});
		}catch (RedisException e){
			callback.fail(e);
			log.error("[ping]" + hostPort, e);
		}
	}

	public  void role(RollCallback callback){

		final CompletableFuture<List<Object>> future = findOrCreateNonSubscribeConnection().async().role().toCompletableFuture();
		future.thenRun(new Runnable() {

			@Override
			public void run() {

				try {
					List<Object> objects = future.get();
					callback.role((String) objects.get(0));
				} catch (Exception e) {
					log.error("[run]" + hostPort, e);
				}
			}
		});
	}

	public  String roleSync() throws Exception {

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

	public interface  RollCallback{

		void role(String role);

		void fail(Exception e);
	}

	public interface SubscribeCallback{

		void message(String channel, String message);

		void fail(Exception e);
	}
}
