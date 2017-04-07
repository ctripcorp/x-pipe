package com.ctrip.xpipe.redis.console.health;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

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

	public synchronized void subscribeIfAbsent(String channel, RedisPubSubListener<String, String> listener) {
		if (!subscribConns.containsKey(channel)) {
			try {
				redis.addListener(new RedisConnectionStateListener() {
					
					@Override
					public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
					}
					
					@Override
					public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
						
					}
					
					@SuppressWarnings("unchecked")
					@Override
					public void onRedisConnected(RedisChannelHandler<?, ?> connection) {
						log.debug("[onRedisConnected]{}", connection);
						if(connection instanceof StatefulRedisPubSubConnection){
							log.info("[onRedisConnected][subscribe]{},{}", hostPort, channel);
							StatefulRedisPubSubConnection<String, String>  pubsubConnection = (StatefulRedisPubSubConnection<String, String>)connection;
							pubsubConnection.async().subscribe(channel);
						}
						
					}
				});
				StatefulRedisPubSubConnection<String, String> pubSub = redis.connectPubSub();
				pubSub.addListener(listener);
				subscribConns.put(channel, pubSub);
			} catch (RuntimeException e) {
				// connect* will throw exception if zk is down at first connect
				log.warn("Error subscribe to redis {}", hostPort);
			}
		}
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

		final CompletableFuture<String> future = findOrCreateNonSubscribeConnection().async().ping().toCompletableFuture();
		future.thenRun(new Runnable() {

			@Override
			public void run() {
				if (future.isDone()) {
					callback.pong(true, future.getNow(null));
				} else {
					callback.pong(false, null);
				}
			}
		});
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
}
