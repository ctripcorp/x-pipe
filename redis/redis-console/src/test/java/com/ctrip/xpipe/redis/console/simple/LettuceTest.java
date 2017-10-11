package com.ctrip.xpipe.redis.console.simple;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import com.lambdaworks.redis.resource.ClientResources;
import com.lambdaworks.redis.resource.DefaultClientResources;
import com.lambdaworks.redis.resource.Delay;
import com.lambdaworks.redis.sentinel.api.StatefulRedisSentinelConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 *         Mar 20, 2017
 */
@SuppressWarnings("unchecked")
public class LettuceTest extends AbstractConsoleTest {

	private String channel = "testChannel";
	private String host = "localhost";
	private int port = 6001;

	private ClientResources clientResources;
	private RedisURI redisURI;

	@Before
	public void beforeLettuceTest() {
		clientResources = DefaultClientResources.builder().reconnectDelay(Delay.constant(5, TimeUnit.SECONDS)).build();
		redisURI = new RedisURI(host, port, 10, TimeUnit.SECONDS);
	}

	@Test
	public void testSentinel(){

		RedisClient redisClient = RedisClient.create(clientResources, redisURI);
		StatefulRedisSentinelConnection<String, String> connection = redisClient.connectSentinel();
//		connection.sync().monitor("mymaster", "127.0.0.1", 6379, 2);
		logger.info("{}", connection.sync().master("mymaster"));
		List<Map<String, String>> mymaster = connection.sync().slaves("mymaster");
		logger.info("{}", mymaster);

	}


	@Test
	public void testConfigRewrite() throws IOException {

		RedisClient redisClient = RedisClient.create(clientResources, redisURI);
		StatefulRedisConnection<String, String> connect = redisClient.connect();
		RedisFuture<String> configRewrite = connect.async().configRewrite();

		configRewrite.whenComplete((str, th) -> {

			logger.info("result:{}", str);
			logger.info("exception:{}", th.getMessage());


		});

		sleep(1000);
	}

	@Test
	public  void testRole(){

		RedisClient redisClient = RedisClient.create(clientResources, redisURI);
		StatefulRedisConnection<String, String> connect = redisClient.connect();

		logger.info("{}", connect.sync().role());

		RedisFuture<List<Object>> role = connect.async().role();

		role.whenComplete((result, th) -> {
			logger.info("{}, ex:{}", result, th);
		});

		sleep(1000);
	}

	@Test
	public void testPubSub(){

		RedisClient redisClient = RedisClient.create(clientResources, redisURI);

		StatefulRedisPubSubConnection<String, String> redisPubSubConnection = redisClient.connectPubSub();

		redisPubSubConnection.async().subscribe(channel);

//		checkConnectionOpen(redisPubSubConnection);

		//noinspection unchecked
		redisPubSubConnection.addListener(new RedisPubSubAdapter(){

			@Override
			public void message(Object channel, Object message) {
				logger.info("{}, {}", channel, message);
			}
		});

		publish();


//		sleep(10000);
//		logger.info("[close conenction]");
//		redisPubSubConnection.close();

	}

	private void checkConnectionOpen(StatefulRedisPubSubConnection<String, String> redisPubSubConnection) {
		scheduled.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {

				logger.info("[run][connection active]{}", redisPubSubConnection.isOpen());

			}
		}, 0, 1, TimeUnit.SECONDS);
	}

	@Test
	public void testLettuce() throws IOException {

//		publish();

		RedisClient redisClient = RedisClient.create(clientResources, redisURI);
		
		redisClient.addListener(new RedisConnectionStateListener() {

			@Override
			public void onRedisConnected(RedisChannelHandler<?, ?> connection) {
				logger.info("[onRedisConnected]{}", connection);
//				@SuppressWarnings("unchecked")
//				StatefulRedisPubSubConnection<String, String> pubsubConnection = (StatefulRedisPubSubConnection<String, String>)connection;
//				pubsubConnection.async().subscribe(channel);
			}

			@Override
			public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
				logger.info("[onRedisDisconnected]{}", connection);
			}

			@Override
			public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
				logger.error("[onRedisExceptionCaught]" + connection, cause);
			}
		});

		try{
			doConnect(redisClient);
		}catch(Exception e){
			logger.error("[testLettuce][fail]", e);
		}

		waitForAnyKeyToExit();
//		connectUntilConnected(redisClient);
	}

	protected void connectUntilConnected(RedisClient redisClient) {

		scheduled.schedule(new AbstractExceptionLogTask() {

			@Override
			protected void doRun() throws Exception {

				try {
					doConnect(redisClient);
				} catch (Exception e) {
					connectUntilConnected(redisClient);
					logger.error("[doRun][reconnect]", e);
				}
			}
		}, 5, TimeUnit.SECONDS);

	}

	protected void doConnect(RedisClient redisClient) {
		StatefulRedisPubSubConnection<String, String> connection = redisClient.connectPubSub();
		connection.addListener(new RedisPubSubAdapter<String, String>() {

			@Override
			public void message(String channel, String message) {
				logger.info("[message]{}, {}", channel, message);
			}
		});
	}

	@SuppressWarnings("unused")
	private void publish() {

		scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

			StatefulRedisConnection<String, String> redisConnection;
			private RedisClient redisClient;
			{
				redisClient = RedisClient.create(clientResources, redisURI);
			}

			@Override
			public void doRun() {

				if (redisConnection == null) {
					redisConnection = redisClient.connect();
				}

				logger.info("[run][publish]{}", channel);
				redisConnection.async().publish(channel, randomString(10));

			}
		}, 0, 5, TimeUnit.SECONDS);
	}

	@After
	public void afterLettuceTest() throws IOException {
//		waitForAnyKeyToExit();
	}

}
