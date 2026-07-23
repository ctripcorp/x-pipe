package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
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

import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_COMMAND_EXECUTOR;
import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_SESSION_NETTY_CLIENT_POOL;
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

    @Autowired
    private AlertManager alertManager;

    @VisibleForTesting
    public static long checkRedisDelaySeconds = 4;

    private DynamicDelayPeriodTask removeUnusedTask;


    @PostConstruct
    public void postConstruct() {
        this.removeUnusedTask = new DynamicDelayPeriodTask("RemoveUnusedInstances",
                this::removeUnusedInstances, config::getSessionRemoveUnusedDelayMillis, scheduled);
        try {
            removeUnusedTask.start();
        } catch (Exception e) {
            logger.error("[postConstruct] start removeUnusedTask fail", e);
            alertManager.alert(null, null, null, ALERT_TYPE.CHECKER_SESSION_MANAGER_FAIL,
                    "session removeUnusedTask start fail: " + e.getMessage());
        }

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                for(RedisSession redisSession : sessions.values()){
                    try {
                        redisSession.check();
                    } catch (Exception e) {
                        logger.error("[check]" + redisSession, e);
                    }
                }
            }
        }, checkRedisDelaySeconds, checkRedisDelaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public synchronized RedisSession findOrCreateSession(Endpoint endpoint) {
        RedisSession session = sessions.get(endpoint);

        if (session == null) {
            session = new RedisSession(endpoint, scheduled, keyedObjectPool, config);
            sessions.put(endpoint, session);
        }

        return session;
    }

    @Override
    public synchronized RedisSession findOrCreateSession(HostPort hostPort) {
        return findOrCreateSession(endpointFactory.getOrCreateEndpoint(hostPort));
    }

    @VisibleForTesting
    protected synchronized void removeUnusedInstances() {
        TransactionMonitor.DEFAULT.logTransactionSwallowException(
                "session.cleanup", "removeUnusedInstances", new Task() {
            @Override
            public void go() {
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
                    try {
                        logger.info("[removeUnusedRedises]Redis: {} not in use, remove from session manager", endpoint);
                        removeSession(endpoint);
                    } catch (Exception e) {
                        logger.warn("[removeUnusedRedises] close session {} failed", endpoint, e);
                    }
                });
            }

            @Override
            public java.util.Map<String, Object> getData() {
                return null;
            }
        });
    }

    protected abstract Set<HostPort> getInUseInstances();


    public synchronized boolean removeSession(Endpoint endpoint) {
        RedisSession session = sessions.remove(endpoint);
        if (session == null) return false;
        session.close();
        return true;
    }


    protected void closeAllConnections() {
        try {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    for (RedisSession session : sessions.values()) {
                        session.close();
                    }
                }
            });
        } catch (Exception e) {
            logger.error("[closeAllConnections] {}", e);
        }
    }

    @PreDestroy
    public void preDestroy(){
        if (removeUnusedTask != null) {
            try {
                removeUnusedTask.stop();
            } catch (Exception e) {
                logger.error("[preDestroy] stop removeUnusedTask fail", e);
            }
        }
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
