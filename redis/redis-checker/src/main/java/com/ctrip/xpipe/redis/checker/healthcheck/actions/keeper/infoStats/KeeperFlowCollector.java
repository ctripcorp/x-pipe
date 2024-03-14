package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
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

    protected ConcurrentMap<String, Map<DcClusterShardKeeper, Long>> hostPort2InputFlow = new ConcurrentHashMap<>();

    @Override
    public void onAction(KeeperInfoStatsActionContext context) {
        try {
            InfoResultExtractor extractor = context.getResult();
            KeeperInstanceInfo info = context.instance().getCheckInfo();
            long keeperFlow = extractor.getKeeperInstantaneousInputKbps().longValue();
            deleteKeeper(info);
            Map<DcClusterShardKeeper, Long> keeperContainerResult = MapUtils.getOrCreate(hostPort2InputFlow, info.getHostPort().getHost(), ConcurrentHashMap::new);
            keeperContainerResult.put(new DcClusterShardKeeper(info.getDcId(), info.getClusterId(), info.getShardId(), extractor.getKeeperActive(), info.getHostPort().getPort()), keeperFlow);
        } catch (Throwable throwable) {
            logger.error("get instantaneous input kbps of keeper:{} error: ", context.instance().getCheckInfo().getHostPort(), throwable);
        }

    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        KeeperInstanceInfo instanceInfo = (KeeperInstanceInfo) action.getActionInstance().getCheckInfo();
        logger.info("stopWatch: {}", new DcClusterShardKeeper(instanceInfo.getDcId(), instanceInfo.getClusterId(), instanceInfo.getShardId(), instanceInfo.isActive(), instanceInfo.getHostPort().getPort()));
        deleteKeeper(instanceInfo);
    }

    private void deleteKeeper(KeeperInstanceInfo instanceInfo) {
        if (hostPort2InputFlow.get(instanceInfo.getHostPort().getHost()) == null) return;
        hostPort2InputFlow.get(instanceInfo.getHostPort().getHost())
                .remove(new DcClusterShardKeeper(instanceInfo.getDcId(), instanceInfo.getClusterId(), instanceInfo.getShardId(), true, instanceInfo.getHostPort().getPort()));
        hostPort2InputFlow.get(instanceInfo.getHostPort().getHost())
                .remove(new DcClusterShardKeeper(instanceInfo.getDcId(), instanceInfo.getClusterId(), instanceInfo.getShardId(), false, instanceInfo.getHostPort().getPort()));
    }

    public ConcurrentMap<String, Map<DcClusterShardKeeper, Long>> getHostPort2InputFlow() {
        return hostPort2InputFlow;
    }
}
