package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper;

import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoListener;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractKeeperLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public abstract class AbstractKeeperInfoCommandActionFactory<T extends AbstractInfoListener, C extends AbstractKeeperInfoCommand>
        extends AbstractKeeperLeaderAwareHealthCheckActionFactory {
    @Autowired
    private List<T> listeners;

    @Override
    public SiteLeaderAwareHealthCheckAction create(KeeperHealthCheckInstance instance) {
        C action = createAction(instance);
        action.addListeners(listeners);
        return action;
    }

    protected abstract C createAction(KeeperHealthCheckInstance instance);

}
