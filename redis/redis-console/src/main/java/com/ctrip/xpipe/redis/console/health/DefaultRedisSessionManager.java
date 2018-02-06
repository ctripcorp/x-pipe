package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.ClientOptions.DisconnectedBehavior;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.SocketOptions;
import com.lambdaworks.redis.resource.ClientResources;
import com.lambdaworks.redis.resource.DefaultClientResources;
import com.lambdaworks.redis.resource.Delay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1,
			XpipeThreadFactory.create(getClass().getSimpleName()));

	@Autowired
	private MetaCache metaCache;

	@VisibleForTesting
	protected ExecutorService executors;

	@VisibleForTesting
	protected ExecutorService pingAndDelayExecutor;

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

		int corePoolSize = 30 * OsUtils.getCpuCount();
		int maxPoolSize =  512;
		DefaultExecutorFactory executorFactory = new DefaultExecutorFactory("RedisSession", corePoolSize, maxPoolSize,
				new ThreadPoolExecutor.AbortPolicy());
		executors = executorFactory.createExecutorService();

		int fixedPoolSize = OsUtils.getCpuCount();
		pingAndDelayExecutor = new DefaultExecutorFactory("Ping-Delay-Executor", fixedPoolSize, fixedPoolSize,
				new ThreadPoolExecutor.CallerRunsPolicy()).createExecutorService();

		scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {
				try {
					removeUnusedRedises();
				} catch (Exception e) {
					logger.error("[removeUnusedRedises]", e);
				}

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

	@VisibleForTesting
	protected void removeUnusedRedises() {
		Set<HostPort> currentStoredRedises = sessions.keySet();
		if(currentStoredRedises.isEmpty())
			return;

		Set<HostPort> redisInUse = getInUseRedises();
		List<HostPort> unusedRedises;
		if(redisInUse == null || redisInUse.isEmpty()) {
			unusedRedises = new LinkedList<>(currentStoredRedises);
		} else {
			unusedRedises = currentStoredRedises.stream()
					.filter(hostPort -> !redisInUse.contains(hostPort))
					.collect(Collectors.toList());
		}
		if(unusedRedises == null || unusedRedises.isEmpty()) {
			return;
		}
		unusedRedises.forEach(hostPort -> {
			RedisSession redisSession = sessions.getOrDefault(hostPort, null);
			if(redisSession != null) {
				logger.info("[removeUnusedRedises]Redis: {} not in use, remove from session manager", hostPort);
				// add try logic to continue working on others
				try {
					redisSession.closeConnection();
				} catch (Exception ignore) {

				}
				sessions.remove(hostPort);
			}
		});
	}


	private Set<HostPort> getInUseRedises() {
		Set<HostPort> redisInUse = new HashSet<>();
		List<DcMeta> dcMetas = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
		if(dcMetas.isEmpty())	return null;
		for (DcMeta dcMeta : dcMetas) {
			if(dcMeta == null)	break;
			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					for (RedisMeta redisMeta : shardMeta.getRedises()) {
						redisInUse.add(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
					}
				}
			}
		}
		return redisInUse;
	}

	@Override
	public RedisSession findOrCreateSession(String host, int port) {

		if(StringUtil.isEmpty(host) || port == 0) {
			throw new IllegalArgumentException("Redis Host/Port can not be empty: " + host + ":" + port);
		}
		HostPort hostPort = new HostPort(host, port);
		RedisSession session = sessions.get(hostPort);

		if (session == null) {
			synchronized (this) {
				session = sessions.get(hostPort);
				if (session == null) {
					session = new RedisSession(findRedisConnection(host, port), hostPort, executors, pingAndDelayExecutor);
					sessions.put(hostPort, session);
				}
			}
		}

		return session;
	}

	public RedisClient findRedisConnection(String host, int port) {
		RedisURI redisUri = new RedisURI(host, port, 2, TimeUnit.SECONDS);
		SocketOptions socketOptions = SocketOptions.builder()
				.connectTimeout(XPipeConsoleConstant.SOCKET_TIMEOUT, TimeUnit.SECONDS)
				.build();
		ClientOptions clientOptions = ClientOptions.builder() //
				.socketOptions(socketOptions)
				.disconnectedBehavior(DisconnectedBehavior.REJECT_COMMANDS)//
				.build();

		RedisClient redis = RedisClient.create(clientResources, redisUri);
		redis.setOptions(clientOptions);

		return redis;
	}

	private void closeAllConnections() {
		try {
			executors.execute(new Runnable() {
				@Override
				public void run() {
					for (RedisSession session : sessions.values()) {
						session.closeConnection();
					}
				}
			});
		} catch (Exception e) {
			logger.error("[closeAllConnections] {}", e);
		}
	}

	public ExecutorService getExecutors() {
		return executors;
	}

	@PreDestroy
	public void preDestroy(){
		closeAllConnections();
		clientResources.shutdown();
		executors.shutdownNow();
	}
}
