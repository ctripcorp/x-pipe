package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.framework.xpipe.redis.utils.ProxyUtil;
import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.ClusterActiveDcKey;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class DefaultOuterClientAggregator implements OuterClientAggregator{

    private Map<ClusterActiveDcKey, Aggregator> clusterAggregators = Maps.newConcurrentMap();

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    private AggregatorPullService aggregatorPullService;

    @Autowired
    private List<HealthStateService> healthStateServices; // loop depend?

    private Random rand = new Random();

    private KeyedOneThreadTaskExecutor<String> clusterOneThreadTaskExecutor;

    private final Logger logger =  LoggerFactory.getLogger(DefaultOuterClientAggregator.class);

    private static final int DELTA = 500;

    @PostConstruct
    public void postConstruct() {
        clusterOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<>(executors);
    }

    @Override
    public void markInstance(ClusterShardHostPort info) {
        ClusterActiveDcKey key = new ClusterActiveDcKey(info.getClusterName(), info.getActiveDc());
        Aggregator aggregator = clusterAggregators.computeIfAbsent(key, clusterActiveDcKey -> {
            Aggregator clusterAggregator = new Aggregator(clusterActiveDcKey, scheduled, checkerConfig.getMarkupInstanceMaxDelayMilli());
            clusterAggregator.start();
            return clusterAggregator;
        });

        aggregator.add(info.getHostPort());
        logger.info("[delayMarkInstance][{}]{}", key, info.getHostPort());
    }

    public class Aggregator {
        private long waitStartTime = 0;
        private String clusterName;
        private String activeDc;
        private Set<HostPort> todo = new HashSet<>();
        private Set<HostPort> doing = new HashSet<>();
        private long markupWaitTimeoutInMills;
        private ScheduledExecutorService scheduled;


        public Aggregator(ClusterActiveDcKey key, ScheduledExecutorService scheduled, long markupWaitTimeoutInMills) {
            this.clusterName = key.getCluster();
            this.activeDc = key.getActiveDc();
            this.scheduled = scheduled;
            this.markupWaitTimeoutInMills = markupWaitTimeoutInMills;
        }


        public synchronized boolean add(HostPort hostPort) {
            if (waitStartTime == 0)
                waitStartTime = System.currentTimeMillis();
            return todo.add(hostPort);
        }

        public synchronized boolean markupWaitTimeout() {
            return System.currentTimeMillis() - waitStartTime > markupWaitTimeoutInMills;
        }

        public void start() {
            if (scheduled != null)
                scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        if (noInstance())
                            return;

                        handleInstances();
                    }
                }, randomMill(), checkInterval(), TimeUnit.MILLISECONDS);
        }

        public synchronized boolean noInstance() {
            return todo.isEmpty() && doing.isEmpty();
        }

        public synchronized Set<HostPort> prepareInstances() {
            doing.addAll(todo);
            todo.clear();
            return doing;
        }

        public synchronized void done() {
            this.doing.clear();
            this.waitStartTime = 0;
        }

        public void handleInstances() {
            logger.info("[handleInstances][{}:{}]", clusterName, activeDc);
            clusterOneThreadTaskExecutor.execute(clusterName, new AggregatorCheckAndSetTask(clusterName, activeDc, this));
        }

        @VisibleForTesting
        synchronized Set<HostPort> getDoing() {
            return doing;
        }

        @VisibleForTesting
        synchronized Set<HostPort> getTodo() {
            return todo;
        }

        @VisibleForTesting
        synchronized long getWaitStartTime() {
            return waitStartTime;
        }
    }

    int randomMill() {
        int delayBase = Math.max(checkerConfig.getMarkInstanceBaseDelayMilli(),
                checkerConfig.getRedisReplicationHealthCheckInterval() + checkerConfig.getCheckerMetaRefreshIntervalMilli());
        int delayMax = Math.max(checkerConfig.getMarkdownInstanceMaxDelayMilli(), delayBase + DELTA);
        return delayBase + rand.nextInt(delayMax - delayBase);
    }

    int checkInterval() {
        int delayBase = Math.max(checkerConfig.getMarkInstanceBaseDelayMilli(),
                checkerConfig.getRedisReplicationHealthCheckInterval() + checkerConfig.getCheckerMetaRefreshIntervalMilli());
        int delayMax = Math.max(checkerConfig.getMarkdownInstanceMaxDelayMilli(), delayBase + DELTA);
        return delayMax - delayBase;
    }

    private Set<HostPortDcStatus> getNeedMarkInstances(String cluster, Set<HostPort> clusterHostPorts) throws Exception {
        logger.info("[aggregator][getNeedMarkInstances][{}]{}", cluster, clusterHostPorts);
        return aggregatorPullService.getNeedAdjustInstances(cluster, clusterHostPorts);
    }

    private void doMarkInstance(String cluster, String activeDc, Set<HostPortDcStatus> needMarkInstances) throws Exception {
        logger.info("[aggregator][doMarkInstance][{}]{}", cluster, needMarkInstances);
        aggregatorPullService.doMarkInstances(cluster, activeDc, needMarkInstances);
    }

    public class AggregatorCheckAndSetTask extends AbstractCommand<Void> {

        private int retry;

        private String cluster;

        private String activeDc;

        private Aggregator aggregator;

        public AggregatorCheckAndSetTask(String cluster, String activeDc, Aggregator aggregator) {
            this(cluster, activeDc, aggregator, 3);
        }

        public AggregatorCheckAndSetTask(String cluster, String activeDc, Aggregator aggregator, int retry){
            this.cluster = cluster;
            this.activeDc = activeDc;
            this.aggregator = aggregator;
            this.retry = retry;
        }

        @Override
        protected void doExecute() throws Exception {

            Set<HostPort> current = aggregator.prepareInstances();
            Set<HostPortDcStatus> instancesToUpdate;

            try {
                instancesToUpdate = getNeedMarkInstances(cluster, current);
            } catch (Throwable th) {
                logger.error("[aggregator][getFail]{}", cluster, th);
                aggregator.done();
                future().setFailure(th);
                return;
            }

            if (instancesToUpdate.isEmpty()) {
                logger.info("[aggregator][getEmpty] skip: {}", cluster);
                aggregator.done();
                future().setSuccess();
            } else {
                Set<HostPortDcStatus> markdownInstances = instancesToUpdate.stream().filter(hostPortDcStatus -> !hostPortDcStatus.isCanRead()).collect(Collectors.toSet());
                if (!markdownInstances.isEmpty() || aggregator.markupWaitTimeout() || dcInstancesAllUp(cluster, current)) {
                    for (HostPortDcStatus hostPortDcStatus : instancesToUpdate) {
                        for (HealthStateService stateService : healthStateServices) {
                            try {
                                stateService.updateLastMarkHandled(
                                        new HostPort(hostPortDcStatus.getHost(), hostPortDcStatus.getPort()),
                                        hostPortDcStatus.isCanRead());
                            } catch (Throwable th) {
                                logger.warn("[aggregator][updateLastMark][fail]{}", cluster, th);
                            }
                        }
                    }

                    for (int i = 0; i < retry; i++) {
                        try {
                            logger.debug("[aggregator][begin] {}", cluster);
                            doMarkInstance(cluster, activeDc, instancesToUpdate);
                            aggregator.done();
                            future().setSuccess();
                            logger.debug("[aggregator][end] {}", cluster);
                            return;
                        } catch (Throwable th) {
                            logger.error("[aggregator][setFail] {} ", cluster, th);
                        }
                    }
                    aggregator.done();
                    future().setFailure(new IllegalStateException("[aggregator][fail] "+ cluster));
                } else {
                    logger.info("[aggregator][allMarkup] continue wait, {}", cluster);
                    future().setSuccess();
                }

            }
        }

        @Override
        public String getName() {
            return "[AggregatorCheckAndSetTask]" + cluster;
        }

        @Override
        protected void doReset() {
        }
    }

    boolean dcInstancesAllUp(String clusterName, Set<HostPort> instances) {
        return aggregatorPullService.dcInstancesAllUp(clusterName, instances);
    }

    @VisibleForTesting
    void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @VisibleForTesting
    void setExecutors(ExecutorService executors) {
        this.executors = executors;
    }

    @VisibleForTesting
    void setHealthStateServices(List<HealthStateService> healthStateServices) {
        this.healthStateServices = healthStateServices;
    }

    @VisibleForTesting
    Aggregator getClusterAggregator(ClusterActiveDcKey clusterActiveDcKey) {
        return this.clusterAggregators.get(clusterActiveDcKey);
    }

}
