package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.AbstractKeeperInfoCommandActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KeeperInfoStatsActionFactory extends AbstractKeeperInfoCommandActionFactory<KeeperInfoStatsActionListener, KeeperInfoStatsAction>
        implements KeeperSupport {

    @Override
    protected KeeperInfoStatsAction createAction(KeeperHealthCheckInstance instance) {
        return new KeeperInfoStatsAction(scheduled, instance, executors);
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return KeeperInfoStatsAction.class;
    }
}
