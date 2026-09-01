package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_EXECUTORS;
import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_SCHEDULED;

@Component
public class InfoReplIdActionFactory implements RedisHealthCheckActionFactory<InfoReplIdAction>, OneWaySupport {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private RedisSessionManager redisSessionManager;

    @Autowired
    private List<InfoReplIdPingActionCollector> collectors;

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(InfoReplIdActionFactory.class);

    @Override
    public InfoReplIdAction create(RedisHealthCheckInstance instance) {
        logger.info("[create] {}", instance.getCheckInfo().getHostPort());
        InfoReplIdAction action = new InfoReplIdAction(scheduled, instance, executors, redisSessionManager);
        collectors.forEach(c -> {
            if (c.supportInstance(instance)) {
                logger.info("[create][add listener] {}, collector={}", instance.getCheckInfo().getHostPort(), c.getClass().getSimpleName());
                action.addListener(c.createInfoReplIdActionListener());
                c.createHealthStatus(instance);
            }
        });
        return action;
    }

    @Override
    public boolean supportInstnace(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        boolean support = metaCache.isCrossRegion(currentDcId, info.getActiveDc())
                && currentDcId.equalsIgnoreCase(info.getDcId());
        logger.info("[supportInstnace] {}, activeDc={}, dcId={}, support={}",
                info.getHostPort(), info.getActiveDc(), info.getDcId(), support);
        return support;
    }

}
