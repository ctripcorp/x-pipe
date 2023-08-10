package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class DefaultKeeperInfoStatsActionListener implements KeeperInfoStatsActionListener, KeeperSupport {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected ConcurrentMap<String, Map<HostPort, Long>> hostPort2InputFlow = new ConcurrentHashMap<>();

    @Override
    public void onAction(KeeperInfoStatsActionContext context) {
        try {
            InfoResultExtractor extractor = context.getResult();
            KeeperInstanceInfo info = context.instance().getCheckInfo();
            long keeperFlow = extractor.getKeeperInstantaneousInputKbps();
            Map<HostPort, Long> keeperContainerResult = MapUtils.getOrCreate(hostPort2InputFlow, info.getHostPort().getHost(), ()->new ConcurrentHashMap<>());
            keeperContainerResult.put(info.getHostPort(), keeperFlow);
        } catch (Throwable throwable) {
            logger.error("get instantaneous input kbps of keeper:{} error: ", context.instance().getCheckInfo().getHostPort(), throwable);
        }

    }

    @Override
    public void stopWatch(HealthCheckAction action) {

    }

}
