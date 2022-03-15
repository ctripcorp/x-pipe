package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.ctrip.xpipe.redis.core.meta.MetaSynchronizer.META_SYNC;

@Component
public class ClusterTypeUpdateEventListener implements ClusterEventListener {
    private static Logger logger = LoggerFactory.getLogger(ClusterTypeUpdateEventListener.class);

    @Autowired
    private ShardService shardService;

    @Autowired
    private SentinelBalanceService sentinelBalanceService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private DcClusterService dcClusterService;

    @Override
    public void update(Object args, Observable observable) {
        try {
            EventType type = (EventType) args;
            if (!(observable instanceof ClusterTypeUpdateEvent) || type != EventType.UPDATE) {
                logger.info("[update] observable object not ClusterTypeUpdateEvent, skip. observable: {}, args: {}",
                        observable.getClass().getName(),
                        args.getClass().getName());
                return;
            }

            ClusterTypeUpdateEvent clusterTypeUpdateEvent = (ClusterTypeUpdateEvent) observable;

            String clusterName = clusterTypeUpdateEvent.getClusterName();
            ClusterType clusterType = clusterTypeUpdateEvent.getClusterType();
            List<ShardTbl> shardTbls = shardService.findAllByClusterName(clusterTypeUpdateEvent.getClusterName());

            if (shardTbls != null) {
                shardTbls.forEach(shardTbl -> {
                    changeShardSentinel(clusterName, shardTbl.getShardName(), clusterType);
                });
            }
        } catch (Throwable e) {
            logger.error("[update] observable: {}, args: {}",
                    observable.getClass().getName(),
                    args.getClass().getName(), e);
        }

    }

    void changeShardSentinel(String clusterName, String shardName, ClusterType clusterType) {
        try {
            List<DcClusterCreateInfo> dcClusterTbls = dcClusterService.findClusterRelated(clusterName);
            if (clusterType.isCrossDc()) {
                changeCrossDcShardSentinel(dcClusterTbls.get(0).getDcName(), clusterName, shardName, clusterType);
            } else {
                changeNonCrossDcShardSentinel(dcClusterTbls, clusterName, shardName, clusterType);
            }
        } catch (Throwable e) {
            logger.error("[changeShardSentinel]{},{},{}", clusterName, shardName, clusterType.name());
        }
    }

    void changeCrossDcShardSentinel(String dcName, String clusterName, String shardName, ClusterType clusterType){
        try {
            List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.find(clusterName, shardName);
            SentinelGroupModel sentinelGroupModel = sentinelBalanceService.selectSentinel(dcName, clusterType);
            if (sentinelGroupModel != null) {
                for (DcClusterShardTbl dcClusterShardTbl : dcClusterShardTbls) {
                    dcClusterShardService.updateDcClusterShard(dcClusterShardTbl.setSetinelId(sentinelGroupModel.getSentinelGroupId()));
                    CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[updateSentinel]%s-%s-%s,sentinelGroupId:%s",clusterType.name(), clusterName, shardName, dcClusterShardTbl.getSetinelId()));
                }
            }
        } catch (Throwable e) {
            logger.error("[changeCrossDcShardSentinel]{},{},{},{}", dcName, clusterName, shardName, clusterType.name());
        }
    }

    void changeNonCrossDcShardSentinel(List<DcClusterCreateInfo> dcClusterTbls, String clusterName, String shardName, ClusterType clusterType) {
        dcClusterTbls.forEach(dcClusterTbl -> {
            changeDcShardSentinel(dcClusterTbl.getDcName(), clusterName, shardName, clusterType);
        });
    }

    void changeDcShardSentinel(String dcName, String clusterName, String shardName, ClusterType clusterType) {
        try {
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcName, clusterName, shardName);
            SentinelGroupModel sentinelGroupModel = sentinelBalanceService.selectSentinel(dcName, clusterType);
            if (dcClusterShardTbl != null && sentinelGroupModel != null) {
                dcClusterShardService.updateDcClusterShard(dcClusterShardTbl.setSetinelId(sentinelGroupModel.getSentinelGroupId()));
                CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[updateSentinel]%s-%s-%s-%s,sentinelGroupId:%s", clusterType.name(), dcName, clusterName, shardName, dcClusterShardTbl.getSetinelId()));
            }
        } catch (Throwable e) {
            logger.error("[changeDcShardSentinel]{},{},{},{}", dcName, clusterName, shardName, clusterType.name(),e);
        }
    }

}
