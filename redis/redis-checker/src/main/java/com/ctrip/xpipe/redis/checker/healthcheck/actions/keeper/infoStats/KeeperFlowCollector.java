package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class KeeperFlowCollector implements KeeperInfoStatsActionListener, KeeperSupport {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected ConcurrentMap<String, Map<DcClusterShardActive, Long>> hostPort2InputFlow = new ConcurrentHashMap<>();

    @Override
    public void onAction(KeeperInfoStatsActionContext context) {
        try {
            InfoResultExtractor extractor = context.getResult();
            KeeperInstanceInfo info = context.instance().getCheckInfo();
            long keeperFlow = (long) extractor.getKeeperInstantaneousInputKbps();
            Map<DcClusterShardActive, Long> keeperContainerResult = MapUtils.getOrCreate(hostPort2InputFlow, info.getHostPort().getHost(), ConcurrentHashMap::new);
            keeperContainerResult.put(new DcClusterShardActive(info.getDcId(), info.getClusterId(), info.getShardId(), info.isActive()), keeperFlow);
        } catch (Throwable throwable) {
            logger.error("get instantaneous input kbps of keeper:{} error: ", context.instance().getCheckInfo().getHostPort(), throwable);
        }

    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        KeeperInstanceInfo instanceInfo = (KeeperInstanceInfo) action.getActionInstance().getCheckInfo();
        logger.debug("stopWatch: {}", new DcClusterShardActive(instanceInfo.getDcId(), instanceInfo.getClusterId(), instanceInfo.getShardId(), instanceInfo.isActive()));
        hostPort2InputFlow.get(instanceInfo.getHostPort().getHost())
                .remove(new DcClusterShardActive(instanceInfo.getDcId(), instanceInfo.getClusterId(), instanceInfo.getShardId(), instanceInfo.isActive()));
    }

    public ConcurrentMap<String, Map<DcClusterShardActive, Long>> getHostPort2InputFlow() {
        return hostPort2InputFlow;
    }
}
