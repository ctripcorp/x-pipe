package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.MetaChangeManager;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
@ConditionalOnProperty(name = {HealthChecker.ENABLED }, matchIfMissing = true)
public class DefaultHealthChecker extends AbstractLifecycle implements HealthChecker {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Autowired
    private MetaChangeManager metaChangeManager;

    @Autowired
    private CheckerConfig checkerConfig;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @PostConstruct
    public void postConstruct() {
        XpipeMeta meta = metaCache.getXpipeMeta();
        if(meta == null) {
            logger.info("[postConstruct] meta cache not ready, do it after one sec");
            scheduled.schedule(new Runnable() {
                @Override
                public void run() {
                    postConstruct();
                }
            }, 1, TimeUnit.SECONDS);
            return;
        }
        init();
    }

    private void init() {
        try {
            LifecycleHelper.initializeIfPossible(this);
        } catch (Exception e) {
            logger.error("[initialize]", e);
        }
        try {
            LifecycleHelper.startIfPossible(this);
        } catch (Exception e) {
            logger.error("[start]", e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        try {
            LifecycleHelper.stopIfPossible(this);
        } catch (Exception e) {
            logger.error("[start]", e);
        }
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        generateHealthCheckInstances();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        metaChangeManager.start();
    }

    @Override
    protected void doStop() throws Exception {
        metaChangeManager.stop();
        super.doStop();
    }


    void generateHealthCheckInstances() {
        XpipeMeta meta = metaCache.getXpipeMeta();

        for(DcMeta dcMeta : meta.getDcs().values()) {
            if(checkerConfig.getIgnoredHealthCheckDc().contains(dcMeta.getId())) {
                continue;
            }
            for(ClusterMeta cluster : dcMeta.getClusters().values()) {

                ClusterType clusterType = ClusterType.lookup(cluster.getType());

                if (dcClusterIsMasterType(clusterType, cluster) && clusterDcIsCurrentDc(cluster)){
                    generateHealthCheckInstances(cluster);
                }
                if (hasSingleActiveDc(clusterType) && isClusterActiveIdcCurrentIdc(cluster)) {
                    generateHealthCheckInstances(cluster);
                }
                if (hasMultipleActiveDcs(clusterType) && isClusterInCurrentIdc(cluster)) {
                    generateHealthCheckInstances(cluster);
                }

            }
        }
    }

    void generateHealthCheckInstances(ClusterMeta clusterMeta){
        for(ShardMeta shard : clusterMeta.getShards().values()) {
            for(RedisMeta redis : shard.getRedises()) {
                instanceManager.getOrCreate(redis);
            }
        }
        instanceManager.getOrCreate(clusterMeta);
    }


    private boolean isClusterActiveIdcCurrentIdc(ClusterMeta cluster) {
        return cluster.getActiveDc().equalsIgnoreCase(currentDcId);
    }

    private boolean isClusterInCurrentIdc(ClusterMeta cluster) {
        if (StringUtil.isEmpty(cluster.getDcs())) return false;

        String[] dcs = cluster.getDcs().split("\\s*,\\s*");
        for (String dc : dcs) {
            if (dc.equalsIgnoreCase(currentDcId)) return true;
        }

        return false;
    }

    private boolean clusterDcIsCurrentDc(ClusterMeta clusterMeta) {
        return clusterMeta.parent().getId().equalsIgnoreCase(currentDcId);
    }

    private boolean dcClusterIsMasterType(ClusterType clusterType, ClusterMeta clusterMeta) {
        if (!StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            ClusterType azGroupType = ClusterType.lookup(clusterMeta.getAzGroupType());
            return clusterType == ClusterType.ONE_WAY && azGroupType == ClusterType.SINGLE_DC;
        }

        return false;
    }

    private boolean hasSingleActiveDc(ClusterType clusterType) {
        return clusterType.supportSingleActiveDC() || clusterType.isCrossDc();
    }

    private boolean hasMultipleActiveDcs(ClusterType clusterType) {
        return clusterType.supportMultiActiveDC() && !clusterType.isCrossDc();
    }

}
