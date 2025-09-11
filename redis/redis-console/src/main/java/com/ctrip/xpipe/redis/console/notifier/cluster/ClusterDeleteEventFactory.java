package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEventListener;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.SentinelGroupService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */

@Component
public class ClusterDeleteEventFactory extends AbstractClusterEventFactory {

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;
    
    @Autowired
    private ShardService shardService;
    
    @Autowired
    private SentinelGroupService sentinelService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;
    
    @Autowired
    private List<ShardDeleteEventListener> shardDeleteEventListeners;

    @Autowired
    private List<ClusterDeleteEventListener> clusterEventListeners;

    @Autowired
    private MetaCache metaCache;

    @Override
    public ClusterEvent createClusterEvent(String clusterName, ClusterTbl clusterTbl) {
        
        ClusterDeleteEvent clusterDeleteEvent = new ClusterDeleteEvent(clusterName, clusterTbl.getClusterOrgId(), executors);
        ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
        if (clusterType.supportMultiActiveDC()) return null;

        clusterDeleteEvent.setClusterType(clusterType);
        List<ShardTbl> shardTbls = shardService.findAllByClusterName(clusterName);
        long activeDcId = clusterService.find(clusterName).getActivedcId();
        String activeDcName = dcService.getDcName(activeDcId);
        if(shardTbls != null) {
            for(ShardTbl shardTbl : shardTbls) {
                logger.info("[createClusterEvent] Create Shard Delete Event: {}", shardTbl);
                Map<Long, SentinelGroupModel> sentinelMap = sentinelService.findByShard(shardTbl.getId());
                ShardDeleteEvent shardEvent = new ShardDeleteEvent(clusterName, shardTbl.getShardName(), executors);
                shardEvent.setClusterType(clusterType);
                try {
                    shardEvent.setShardMonitorName(metaCache.getSentinelMonitorName(clusterName, shardTbl.getShardName()));
                } catch (Exception e) {
                    logger.warn("[createClusterEvent]", e);
                    shardEvent.setShardMonitorName(SentinelUtil.getSentinelMonitorName(clusterName,
                            shardTbl.getSetinelMonitorName(), activeDcName));
                }
                shardEvent.setShardSentinels(getShardSentinelAddress(sentinelMap));
                shardDeleteEventListeners
                        .forEach(shardDeleteEventListener -> shardEvent.addObserver(shardDeleteEventListener));
                clusterDeleteEvent.addShardEvent(shardEvent);
            }
        }
        clusterEventListeners
                .forEach(clusterDeleteEventListener -> clusterDeleteEvent.addObserver(clusterDeleteEventListener));

        return clusterDeleteEvent;
    }
    
    private String getShardSentinelAddress(Map<Long, SentinelGroupModel> sentinelMap) {
        if(sentinelMap.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        for(SentinelGroupModel sentinelTbl : sentinelMap.values()) {
            sb.append(sentinelTbl.getSentinelsAddressString()).append(",");
        }
        return sb.deleteCharAt(sb.length()-1).toString();
    }

    @VisibleForTesting
    protected ClusterDeleteEventFactory setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }
}
