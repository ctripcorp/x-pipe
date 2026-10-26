package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
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
public class KeeperStateActionFactory implements KeeperHealthCheckActionFactory<KeeperStateAction> {

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    private KeeperStateActionController controller;

    @Autowired(required = false)
    private List<KeeperStateActionListener> listeners = Collections.emptyList();

    @Override
    public KeeperStateAction create(KeeperHealthCheckInstance instance) {
        KeeperStateAction action = new KeeperStateAction(scheduled, instance, executors, checkerConfig);
        for (KeeperStateActionListener listener : listeners) {
            if (listener.supportInstance(instance)) {
                action.addListener(listener);
            }
        }
        action.addController(controller);
        return action;
    }

    @Override
    public void destroy(KeeperStateAction action) throws Exception {
        LifecycleHelper.stopIfPossible(action);
    }
}
