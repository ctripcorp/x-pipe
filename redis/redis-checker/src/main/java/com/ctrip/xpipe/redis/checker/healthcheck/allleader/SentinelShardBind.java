package com.ctrip.xpipe.redis.checker.healthcheck.allleader;


import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel.DefaultSentinelBindTask;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel.SentinelBindTask;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;


public class SentinelShardBind extends AbstractAllCheckerLeaderTask {

    static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    private MetaCache metaCache;

    private CheckerConfig config;

    private SentinelManager sentinelManager;

    private ExecutorService executors;

    private CheckerConsoleService checkerConsoleService;

    private Map<ClusterType, SentinelBindTask> bindTasks = Maps.newConcurrentMap();

    @Autowired
    public SentinelShardBind(MetaCache metaCache, CheckerConfig config, SentinelManager sentinelManager, ExecutorService executors, CheckerConsoleService checkerConsoleService) {
        this.metaCache = metaCache;
        this.config = config;
        this.sentinelManager = sentinelManager;
        this.executors = executors;
        this.checkerConsoleService = checkerConsoleService;
    }

    @Override
    public void doTask() {
        for (String type : config.getOuterClusterTypes()) {
            try {
                ClusterType clusterType = ClusterType.lookup(type);
                SentinelBindTask bindTask = bindTasks.get(clusterType);
                if (bindTask != null) {
                    if (!bindTask.future().isDone()) {
                        logger.debug("sentinels of cluster type: {} task running", clusterType);
                        bindTask.future().cancel(true);
                    }
                }

                DcMeta dcMeta = metaCache.getXpipeMeta().getDcs().get(currentDcId);
                if (dcMeta == null)
                    continue;

                SentinelBindTask task = new DefaultSentinelBindTask(sentinelManager, dcMeta, clusterType, checkerConsoleService, config);
                bindTasks.put(clusterType, task);
                task.execute(executors);
            } catch (Exception e) {
                logger.error("bind sentinel of type: {}", type, e);
            }
        }
    }


    @Override
    public int getDelay() {
       return config.sentinelBindTimeoutMilli();
    }

    @Override
    public boolean shouldCheck() {
        return config.shouldBindOuterClusterShardAndSentinel();
    }



    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return new ArrayList<>();
    }
}
