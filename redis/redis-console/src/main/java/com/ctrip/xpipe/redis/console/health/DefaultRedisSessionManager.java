package com.ctrip.xpipe.redis.console.health;

import java.util.concurrent.*;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.metric.HostPort;
import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.ClientOptions.DisconnectedBehavior;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.resource.ClientResources;
import com.lambdaworks.redis.resource.DefaultClientResources;
import com.lambdaworks.redis.resource.Delay;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 6:42:01 PM
 */
@Component
public class DefaultRedisSessionManager implements RedisSessionManager {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private ConcurrentMap<HostPort, RedisSession> sessions = new ConcurrentHashMap<>();

	private ClientResources clientResources;

	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getClass().getSimpleName()));

	public DefaultRedisSessionManager() {
		this(1);
	}

	public DefaultRedisSessionManager(int reconnectDelaySeconds) {
		clientResources = DefaultClientResources.builder()//
				.reconnectDelay(Delay.constant(reconnectDelaySeconds, TimeUnit.SECONDS))//
				.build();
	}

	@PostConstruct
	public void postConstruct(){

		scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {
				for(RedisSession redisSession : sessions.values()){
					try{
						redisSession.check();
					}catch (Exception e){
						logger.error("[check]" + redisSession, e);
					}
				}
			}
		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
	public RedisSession findOrCreateSession(String host, int port) {

		HostPort hostPort = new HostPort(host, port);
		RedisSession session = sessions.get(hostPort);

		if (session == null) {
			synchronized (this) {
				session = sessions.get(hostPort);
				if (session == null) {
					session = new RedisSession(findRedisConnection(host, port), hostPort);
					sessions.put(hostPort, session);
				}
			}
		}

		return session;
	}

	public RedisClient findRedisConnection(String host, int port) {
		RedisURI redisUri = new RedisURI(host, port, 2, TimeUnit.SECONDS);

		ClientOptions clientOptions = ClientOptions.builder() //
				.disconnectedBehavior(DisconnectedBehavior.REJECT_COMMANDS)//
				.build();

		RedisClient redis = RedisClient.create(clientResources, redisUri);
		redis.setOptions(clientOptions);

		return redis;
	}

	@PreDestroy
	public void preDestroy(){
		scheduled.shutdownNow();
		clientResources.shutdown();
	}
}
