package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthStatusManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */

@Component
public class DefaultHealthStatusManager implements HealthStatusManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthStatusManager.class);

    public static final long PING_DOWN_AFTER_MILLI = 30 * 1000;

    public static final long DELAY_MARK_DOWN_BOTTOM_LINE_MILLI = 1000 * 60 * 60;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private List<DelayHealthEventProcessor> delayEventProcessors;

    @Autowired
    private List<PingHealthEventProcessor> pingEventProcessors;

    @Autowired
    private AlertManager alertManager;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Override
    public void markDown(RedisHealthCheckInstance instance, MarkDownReason reason) {
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                getMarkDownWorker(reason).doMarkDown(instance);
            }
        });
    }

    @Override
    public void markUp(RedisHealthCheckInstance instance, MarkUpReason reason) {
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                getMarkUpWorkder(reason).doMarkUp(instance);
            }
        });
    }

    private MarkDownWorker getMarkDownWorker(MarkDownReason markDownReason) {
        if(markDownReason.equals(MarkDownReason.LAG)) {
            return delayMarkDownWorker;
        } else if(markDownReason.equals(MarkDownReason.PING_FAIL)) {
            return pingMarkDownWorker;
        }
        throw new IllegalStateException("No mark down worker for: " + markDownReason);
    }

    private MarkUpWorker getMarkUpWorkder(MarkUpReason markUpReason) {
        if(markUpReason.equals(MarkUpReason.DELAY_HEALTHY)) {
            return delayMarkUpWorker;
        } else if(markUpReason.equals(MarkUpReason.PING_OK)) {
            return pingMarkUpWorker;
        }
        throw new IllegalStateException("No mark up worker for: " + markUpReason);
    }


    private DelayMarkDownWorker delayMarkDownWorker = new DelayMarkDownWorker();

    private DelayMarkUpWorker delayMarkUpWorker = new DelayMarkUpWorker();

    class DelayMarkDownWorker implements MarkDownWorker {

        @Override
        public void doMarkDown(RedisHealthCheckInstance instance) {
            if(!shouldMarkDown(instance)) {
                RedisInstanceInfo info = instance.getRedisInstanceInfo();
                logger.info("[doMarkDown] Do not mark down instance in dc-cluster: {}", info);
                alertManager.alert(info.getClusterId(), info.getShardId(), info.getHostPort(),
                        ALERT_TYPE.INSTANCE_LAG_NOT_MARK_DOWN, info.getDcId());
                return;
            }
            for(DelayHealthEventProcessor processor : delayEventProcessors) {
                processor.markDown(instance);
            }
        }

        private boolean shouldMarkDown(RedisHealthCheckInstance instance) {
            RedisInstanceInfo info = instance.getRedisInstanceInfo();
            Pair dcCluster = new Pair<>(info.getDcId(), info.getClusterId());
            // 1. ignored if configured, 2. beyond bottom_line, mark down even if configured
            boolean ignored = consoleConfig.getDelayWontMarkDownClusters().contains(dcCluster);
            if(ignored) {
                long lastDelayMilli = instance.getHealthCheckContext().getDelayContext().lastTimeDelayMilli();
                long downTimeMilli = System.currentTimeMillis() - lastDelayMilli;
                return downTimeMilli >= DELAY_MARK_DOWN_BOTTOM_LINE_MILLI;
            }
            return true;
        }
    }

    class DelayMarkUpWorker implements MarkUpWorker {

        @Override
        public void doMarkUp(RedisHealthCheckInstance instance) {
            for(DelayHealthEventProcessor processor : delayEventProcessors) {
                processor.markUp(instance);
            }
        }
    }

    private PingMarkDownWorker pingMarkDownWorker = new PingMarkDownWorker();

    private PingMarkUpWorker pingMarkUpWorker = new PingMarkUpWorker();

    class PingMarkDownWorker implements MarkDownWorker {

        @Override
        public void doMarkDown(RedisHealthCheckInstance instance) {
            long lastPongMilli = instance.getHealthCheckContext().getPingContext().lastPongTimeMilli();
            if(System.currentTimeMillis() - lastPongMilli >= PING_DOWN_AFTER_MILLI) {
                for(PingHealthEventProcessor processor : pingEventProcessors) {
                    processor.markDown(instance);
                }
            }
        }
    }

    class PingMarkUpWorker implements MarkUpWorker {

        @Override
        public void doMarkUp(RedisHealthCheckInstance instance) {
            for(PingHealthEventProcessor processor : pingEventProcessors) {
                processor.markUp(instance);
            }
        }
    }
}
