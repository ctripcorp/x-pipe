package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEventListener;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */

@Component
public class ClusterDeleteEventFactory extends AbstractClusterEventFactory {

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executors;
    
    @Autowired
    private ShardService shardService;
    
    @Autowired
    private SentinelService sentinelService;
    
    @Autowired
    private List<ShardDeleteEventListener> shardDeleteEventListeners;

    @Autowired
    private List<ClusterDeleteEventListener> clusterEventListeners;

    @Override
    public ClusterEvent createClusterEvent(String clusterName) {
        
        ClusterDeleteEvent clusterDeleteEvent = new ClusterDeleteEvent(clusterName, executors);
        List<ShardTbl> shardTbls = shardService.findAllByClusterName(clusterName);
        if(shardTbls != null) {
            for(ShardTbl shardTbl : shardTbls) {
                logger.info("[createClusterEvent] Create Shard Delete Event: {}", shardTbl);
                Map<Long, SetinelTbl> sentinelMap = sentinelService.findByShard(shardTbl.getId());
                ShardDeleteEvent shardEvent = new ShardDeleteEvent(clusterName, shardTbl.getShardName(), executors);
                shardEvent.setShardMonitorName(shardTbl.getSetinelMonitorName());
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
    
    private String getShardSentinelAddress(Map<Long, SetinelTbl> sentinelMap) {
        if(sentinelMap.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        for(SetinelTbl sentinelTbl : sentinelMap.values()) {
            sb.append(sentinelTbl.getSetinelAddress()).append(",");
        }
        return sb.deleteCharAt(sb.length()-1).toString();
    }
}
