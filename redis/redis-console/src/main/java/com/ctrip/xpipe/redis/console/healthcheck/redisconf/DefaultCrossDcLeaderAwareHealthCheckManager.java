package com.ctrip.xpipe.redis.console.healthcheck.redisconf;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */

@Component
public class DefaultCrossDcLeaderAwareHealthCheckManager implements CrossDcLeaderAwareHealthCheckManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private List<CrossDcLeaderAwareHealthCheckActionFactory> factories;

    @Autowired
    private HealthCheckInstanceManager healthCheckInstanceManager;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Override
    public void registerTo(RedisHealthCheckInstance instance) {
        new SafeLoop<CrossDcLeaderAwareHealthCheckActionFactory>(factories) {
            @Override
            void doRun0(CrossDcLeaderAwareHealthCheckActionFactory factory) throws Exception{
                if(exists(instance, factory.support())) {
                    return;
                }
                HealthCheckAction action = factory.create(instance);
                instance.register(action);
                LifecycleHelper.initializeIfPossible(action);
                LifecycleHelper.startIfPossible(action);
            }

            @Override
            String getInfo(CrossDcLeaderAwareHealthCheckActionFactory factory) {
                return factory.support().getSimpleName();
            }
        }.run();

    }

    @Override
    public void removeFrom(RedisHealthCheckInstance instance) {
        new SafeLoop<HealthCheckAction>(instance.getHealthCheckActions().values()) {
            @Override
            void doRun0(HealthCheckAction action) throws Exception {
                if(action instanceof CrossDcLeaderAwareHealthCheckAction) {
                    instance.unregister(action);
                    LifecycleHelper.stopIfPossible(action);
                }
            }
        }.run();
    }

    @Override
    public void isCrossDcLeader() {
        new SafeLoop<RedisHealthCheckInstance>(healthCheckInstanceManager.getAllRedisInstance()) {
            @Override
            public void doRun0(RedisHealthCheckInstance instance) {
                registerTo(instance);
            }
        }.run();
    }

    @Override
    public void notCrossDcLeader() {
        new SafeLoop<RedisHealthCheckInstance>(healthCheckInstanceManager.getAllRedisInstance()) {
            @Override
            public void doRun0(RedisHealthCheckInstance instance) {
                removeFrom(instance);
            }
        }.run();
    }

    private boolean exists(RedisHealthCheckInstance instance, Class actionClazz) {
        return instance.getHealthCheckActions().keySet().contains(actionClazz);
    }

    abstract class SafeLoop<T> {

        private Collection<T> collection;

        public SafeLoop(Collection<T> collection) {
            this.collection = collection;
        }

        public void run() {
            for(T t : collection) {
                executors.execute(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        try {
                            doRun0(t);
                        } catch (Exception e) {
                            logger.error("[SafeLoop][{}]", getInfo(t), e);
                        }
                    }
                });

            }
        }

        abstract void doRun0(T t) throws Exception;

        String getInfo(T t) {
            return t.toString();
        }
    }
}
