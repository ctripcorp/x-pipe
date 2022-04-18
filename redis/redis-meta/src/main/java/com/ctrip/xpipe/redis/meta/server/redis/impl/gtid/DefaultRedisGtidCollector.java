package com.ctrip.xpipe.redis.meta.server.redis.impl.gtid;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.job.RedisGtidCollectJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author ayq
 * <p>
 * 2022/4/13 15:53
 */
public class DefaultRedisGtidCollector extends AbstractLifecycle implements RedisGtidCollector, TopElement {

    private int redisStateManagerIntervalSeconds = Integer.parseInt(System.getProperty("REDIS_GTID_COLLECTOR_INTERVAL_SECONDS", "5"));

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Autowired
    private DcMetaCache dcMetaCache;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    @Resource(name = MetaServerContextConfig.CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool clientPool;

    private ScheduledFuture<?> future;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        future = scheduled.scheduleWithFixedDelay(new RedisGtidCollectTask(), redisStateManagerIntervalSeconds, redisStateManagerIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        future.cancel(true);
    }

    class RedisGtidCollectTask extends AbstractExceptionLogTask {

        @Override
        protected void doRun() throws Exception {
            for(Long clusterDbId : currentMetaManager.allClusters()) {
                if (!ClusterType.HETERO.equals(dcMetaCache.getClusterType(clusterDbId))) {
                    continue;
                }

                ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterDbId);
                if(clusterMeta == null){
                    logger.warn("[doRun][cluster null]cluster_{}", clusterDbId);
                    return;
                }

                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    RedisGtidCollectJob redisGtidCollectJob = new RedisGtidCollectJob(clusterDbId, shardMeta.getDbId(),
                            dcMetaCache, scheduled, clientPool);
                    redisGtidCollectJob.execute(executors);
                }
            }
        }
    }
}
