package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_COMMAND_EXECUTOR;
import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_SESSION_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 6:42:01 PM
 */
@Component
public class DefaultRedisSessionManager implements RedisSessionManager {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private ConcurrentMap<Endpoint, RedisSession> sessions = new ConcurrentHashMap<>();

	@Autowired
	private MetaCache metaCache;

	@Autowired
	private HealthCheckEndpointFactory endpointFactory;

	@Resource(name = REDIS_SESSION_NETTY_CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;

	@Resource(name = REDIS_COMMAND_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Resource(name = GLOBAL_EXECUTOR)
	private ExecutorService executors;

	@Autowired
	private CheckerConfig config;

	@VisibleForTesting
	public static long checkUnusedRedisDelaySeconds = 4;

	@PostConstruct
	public void postConstruct(){
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
		}, checkUnusedRedisDelaySeconds, checkUnusedRedisDelaySeconds, TimeUnit.SECONDS);
	}

    @Override
    public RedisSession findOrCreateSession(Endpoint endpoint) {
		RedisSession session = sessions.get(endpoint);

		if (session == null) {
			synchronized (this) {
				session = sessions.get(endpoint);
				if (session == null) {
					session = new RedisSession(endpoint, scheduled, keyedObjectPool, config);
					sessions.put(endpoint, session);
				}
			}
		}

		return session;
    }

	@Override
	public RedisSession findOrCreateSession(HostPort hostPort) {
		return findOrCreateSession(endpointFactory.getOrCreateEndpoint(hostPort));
	}

	@VisibleForTesting
	protected void removeUnusedRedises() {
		Set<Endpoint> currentStoredRedises = sessions.keySet();
		if(currentStoredRedises.isEmpty())
			return;

		Set<HostPort> redisInUse = getInUseRedises();
		if(redisInUse == null || redisInUse.isEmpty()) {
			return;
		}
		List<Endpoint> unusedRedises = new LinkedList<>();

		for(Endpoint endpoint : currentStoredRedises) {
			if(!redisInUse.contains(new HostPort(endpoint.getHost(), endpoint.getPort()))) {
				unusedRedises.add(endpoint);
			}
		}

		if(unusedRedises.isEmpty()) {
			return;
		}
		unusedRedises.forEach(endpoint -> {
			RedisSession redisSession = sessions.getOrDefault(endpoint, null);
			if(redisSession != null) {
				logger.info("[removeUnusedRedises]Redis: {} not in use, remove from session manager", endpoint);
				// add try logic to continue working on others
				try {
					redisSession.closeConnection();
				} catch (Exception ignore) {

				}
				sessions.remove(endpoint);
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

	@PreDestroy
	public void preDestroy(){
		closeAllConnections();
	}

	public DefaultRedisSessionManager setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
		this.keyedObjectPool = keyedObjectPool;
		return this;
	}

	public DefaultRedisSessionManager setScheduled(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
		return this;
	}

	public DefaultRedisSessionManager setExecutors(ExecutorService executors) {
		this.executors = executors;
		return this;
	}

	public DefaultRedisSessionManager setEndpointFactory(HealthCheckEndpointFactory endpointFactory) {
		this.endpointFactory = endpointFactory;
		return this;
	}

	public void setConfig(CheckerConfig config) {
		this.config = config;
	}
}
