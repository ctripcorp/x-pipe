package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.utils.StringUtil;
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
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig.REQUEST_RESPONSE_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig.SUBSCRIBE_NETTY_CLIENT_POOL;

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

	@Resource(name = SUBSCRIBE_NETTY_CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool subscrNettyClientPool;

	@Resource(name = REQUEST_RESPONSE_NETTY_CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool reqResNettyClientPool;

	@Resource(name = ConsoleContextConfig.REDIS_COMMAND_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
	private ExecutorService executors;

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
		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
	public RedisSession findOrCreateSession(String host, int port) {

		if(StringUtil.isEmpty(host) || port == 0) {
			throw new IllegalArgumentException("Redis Host/Port can not be empty: " + host + ":" + port);
		}
		return findOrCreateSession(new DefaultEndPoint(host, port));
	}

    @Override
    public RedisSession findOrCreateSession(Endpoint endpoint) {
		RedisSession session = sessions.get(endpoint);

		if (session == null) {
			synchronized (this) {
				session = sessions.get(endpoint);
				if (session == null) {
					session = new RedisSession(endpoint, scheduled, reqResNettyClientPool, subscrNettyClientPool);
					sessions.put(endpoint, session);
				}
			}
		}

		return session;
    }

	@VisibleForTesting
	protected void removeUnusedRedises() {
		Set<Endpoint> currentStoredRedises = sessions.keySet();
		if(currentStoredRedises.isEmpty())
			return;

		Set<Endpoint> redisInUse = getInUseRedises();
		List<Endpoint> unusedRedises;
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


	private Set<Endpoint> getInUseRedises() {
		Set<Endpoint> redisInUse = new HashSet<>();
		List<DcMeta> dcMetas = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
		if(dcMetas.isEmpty())	return null;
		for (DcMeta dcMeta : dcMetas) {
			if(dcMeta == null)	break;
			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					for (RedisMeta redisMeta : shardMeta.getRedises()) {
						redisInUse.add(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort()));
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

	public DefaultRedisSessionManager setSubscrNettyClientPool(XpipeNettyClientKeyedObjectPool subscrNettyClientPool) {
		this.subscrNettyClientPool = subscrNettyClientPool;
		return this;
	}

	public DefaultRedisSessionManager setReqResNettyClientPool(XpipeNettyClientKeyedObjectPool reqResNettyClientPool) {
		this.reqResNettyClientPool = reqResNettyClientPool;
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
}
