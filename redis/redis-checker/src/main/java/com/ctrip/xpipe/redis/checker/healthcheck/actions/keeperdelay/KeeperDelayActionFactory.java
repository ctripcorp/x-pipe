package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_EXECUTORS;
import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_SCHEDULED;

@Component
public class KeeperDelayActionFactory implements KeeperHealthCheckActionFactory<KeeperDelayAction> {

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private FoundationService foundationService;

    @Autowired
    private KeeperCapabilityCache capabilityCache;

    @Autowired
    private KeeperDelayActionController controller;

    @Autowired(required = false)
    private List<KeeperDelayActionListener> listeners = Collections.emptyList();

    @Override
    public KeeperDelayAction create(KeeperHealthCheckInstance instance) {
        KeeperDelayAction action = new KeeperDelayAction(
                scheduled, instance, executors, foundationService, capabilityCache);
        for (KeeperDelayActionListener listener : listeners) {
            if (listener.supportInstance(instance)) {
                action.addListener(listener);
            }
        }
        action.addController(controller);
        return action;
    }

    @Override
    public void destroy(KeeperDelayAction action) throws Exception {
        LifecycleHelper.stopIfPossible(action);
    }
}
