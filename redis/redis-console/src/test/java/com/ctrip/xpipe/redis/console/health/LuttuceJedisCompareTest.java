package com.ctrip.xpipe.redis.console.health;

import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.ClientOptions.DisconnectedBehavior;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import com.lambdaworks.redis.resource.ClientResources;
import com.lambdaworks.redis.resource.DefaultClientResources;
import com.lambdaworks.redis.resource.Delay;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.TimeUnit;

/**
 * @author marsqing
 *
 *         Dec 9, 2016 12:03:20 PM
 */
@SuppressWarnings("resource")
public class LuttuceJedisCompareTest {

	public static void main(String[] args) throws Exception {
		final String host = "127.0.0.1";
		final int port = 6379;
		final String channel = "xxx";

		final RedisClient c = findRedisConnection(host, port);

		boolean subViaJedis = false;
		boolean pubViaJedis = false;

		if (subViaJedis) {
			new Thread(new Runnable() {
				public void run() {
					Jedis c = new Jedis(host, port);
					c.subscribe(new JedisPubSub() {

						@Override
						public void onMessage(String channel, String message) {
							System.out.println((System.nanoTime() - Long.parseLong(message)) / 1000);
						}

					}, channel);
				}
			}).start();
		} else {
			new Thread(new Runnable() {
				public void run() {
					StatefulRedisPubSubConnection<String, String> sub = c.connectPubSub();
					sub.addListener(new RedisPubSubAdapter<String, String>() {

						@Override
						public void message(String channel, String message) {
							System.out.println((System.nanoTime() - Long.parseLong(message)) / 1000);
						}
					});
					sub.sync().subscribe(channel);
				}
			}).start();
		}

		if (pubViaJedis) {

			Jedis jc = new Jedis(host, port);
			while (true) {
				jc.publish(channel, "" + System.nanoTime());
				Thread.sleep(1000);
			}
		} else {

			StatefulRedisConnection<String, String> pub = c.connect();

			RedisAsyncCommands<String, String> conn = pub.async();
			while (true) {
				conn.publish(channel, "" + System.nanoTime());
				Thread.sleep(1000);
			}
		}
	}

	private static RedisClient findRedisConnection(String host, int port) {
		ClientResources clientResources = DefaultClientResources.builder()//
				.reconnectDelay(Delay.constant(10, TimeUnit.SECONDS))//
				.build();

		RedisURI redisUri = new RedisURI(host, port, 2, TimeUnit.SECONDS);

		ClientOptions clientOptions = ClientOptions.builder() //
				.disconnectedBehavior(DisconnectedBehavior.REJECT_COMMANDS)//
				.build();

		RedisClient redis = RedisClient.create(clientResources, redisUri);
		redis.setOptions(clientOptions);

		return redis;
	}

	@SuppressWarnings("unused")
	private static void subKeyspaceEvents() throws Exception {
		new Thread(new Runnable() {
			public void run() {
				Jedis c = new Jedis("127.0.0.1", 6379);
				c.psubscribe(new JedisPubSub() {

					@Override
					public void onPMessage(String pattern, String channel, String message) {
						System.out.println((System.nanoTime() - Long.parseLong(message)) / 1000);
					}

				}, "__key*__:*");
			}
		}).start();

		Jedis c = new Jedis("127.0.0.1", 6379);
		while (true) {
			c.set("" + System.nanoTime(), "");
			Thread.sleep(1000);
		}
	}

}
