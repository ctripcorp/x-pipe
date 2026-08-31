package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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

    @Override
    public InfoReplIdAction create(RedisHealthCheckInstance instance) {
        InfoReplIdAction action = new InfoReplIdAction(scheduled, instance, executors, redisSessionManager);
        collectors.forEach(c -> {
            if (c.supportInstance(instance)) {
                action.addListener(c.createInfoReplIdActionListener());
                c.createHealthStatus(instance);
            }
        });
        return action;
    }

    @Override
    public boolean supportInstnace(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        return metaCache.isCrossRegion(currentDcId, info.getActiveDc())
                && currentDcId.equalsIgnoreCase(info.getDcId());
    }

}
