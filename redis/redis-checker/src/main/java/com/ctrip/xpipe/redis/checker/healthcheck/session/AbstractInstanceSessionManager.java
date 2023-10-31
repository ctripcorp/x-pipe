package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_COMMAND_EXECUTOR;
import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_SESSION_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.redis.core.meta.comparator.KeeperContainerMetaComparator.getMonitorRedisMeta;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author yu
 * <p>
 * 2023/10/31
 */
public abstract class AbstractInstanceSessionManager implements InstanceSessionManager{

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private ConcurrentMap<Endpoint, RedisSession> sessions = new ConcurrentHashMap<>();

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    protected CheckerConsoleService checkerConsoleService;

    @Autowired
    protected CheckerConfig checkerConfig;

    @Autowired
    private HealthCheckEndpointFactory endpointFactory;

    @Resource(name = REDIS_SESSION_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Resource(name = REDIS_COMMAND_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    protected String currentDcId = FoundationService.DEFAULT.getDataCenter();

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
                    removeUnusedInstances();
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
    protected void removeUnusedInstances() {
        Set<Endpoint> currentStoredRedises = sessions.keySet();
        if(currentStoredRedises.isEmpty())
            return;

        Set<HostPort> redisInUse = getInUseInstances();
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

    public abstract Set<HostPort> getInUseInstances();

    @VisibleForTesting
    protected void getSessionsForKeeper(DcMeta dcMeta, DcMeta allDcMeta, Set<HostPort> InstanceInUse, boolean isRedis) {
        Set<Long> set = dcMeta.getKeeperContainers().stream().map(KeeperContainerMeta::getId).collect(Collectors.toSet());
        allDcMeta.getClusters().values().forEach(clusterMeta -> {
            for (ShardMeta shardMeta : clusterMeta.getAllShards().values()){
                if (shardMeta.getKeepers() == null || shardMeta.getKeepers().isEmpty()) continue;
                shardMeta.getKeepers().forEach(keeperMeta -> {
                    if (set.contains(keeperMeta.getKeeperContainerId())) {
                        RedisMeta monitorRedis;
                        if (!isRedis) {
                            InstanceInUse.add(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
                        } else if ((monitorRedis = getMonitorRedisMeta(shardMeta.getRedises())) != null){
                            InstanceInUse.add(new HostPort(monitorRedis.getIp(), monitorRedis.getPort()));
                        }
                    }
                });
            }
        });
    }

    protected DcMeta getCurrentDcAllMeta(String dcId) {
        try {
            return checkerConsoleService.getXpipeAllDCMeta(checkerConfig.getConsoleAddress(), dcId)
                    .getDcs().get(dcId);
        } catch (Throwable th) {
            logger.error("[getCurrentDcAllMeta] get all dc meta of dc {} fail!", dcId, th);
        }
        return null;
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

    public AbstractInstanceSessionManager setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
        return this;
    }

    public AbstractInstanceSessionManager setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

    public AbstractInstanceSessionManager setExecutors(ExecutorService executors) {
        this.executors = executors;
        return this;
    }

    public AbstractInstanceSessionManager setEndpointFactory(HealthCheckEndpointFactory endpointFactory) {
        this.endpointFactory = endpointFactory;
        return this;
    }

    public void setConfig(CheckerConfig config) {
        this.config = config;
    }

}
