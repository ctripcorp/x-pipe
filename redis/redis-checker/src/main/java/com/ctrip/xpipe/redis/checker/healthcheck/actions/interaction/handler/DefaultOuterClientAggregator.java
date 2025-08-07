package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.migration.OuterClientService.HostPortDcStatus;
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
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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
            try {
                clusterAggregator.start();
            } catch (Throwable th) {
                logger.error("[aggregator][{}:{}][markInstance]start aggregator failed: {}", key.getCluster(), key.getActiveDc(), info.getHostPort(), th);
            }
            return clusterAggregator;
        });

        aggregator.add(info.getHostPort());
        logger.info("[aggregator][{}:{}][delayMarkInstance]{}", key.getCluster(), key.getActiveDc(), info.getHostPort());
    }

    static class StartTimeAndInstances {
        private long waitStartTime = 0;
        private final long markupWaitTimeoutInMills;
        private final Set<HostPort> instances = new HashSet<>();

        public StartTimeAndInstances(long markupWaitTimeoutInMills) {
            this.markupWaitTimeoutInMills = markupWaitTimeoutInMills;
        }

        public long getWaitStartTime() {
            return waitStartTime;
        }

        public Set<HostPort> getInstances() {
            return instances;
        }

        public void addInstance(HostPort hostPort) {
            if (this.waitStartTime == 0)
                this.waitStartTime = System.currentTimeMillis();
            this.instances.add(hostPort);
        }

        public void merge(StartTimeAndInstances other) {
            if (waitStartTime == 0) {
                this.waitStartTime = other.waitStartTime;
            }
            this.instances.addAll(other.instances);
        }

        public void clear() {
            this.waitStartTime = 0;
            this.instances.clear();
        }

        public boolean timeout() {
            if (waitStartTime == 0)
                return false;
            return System.currentTimeMillis() - waitStartTime > markupWaitTimeoutInMills;
        }
    }

    public class Aggregator {
        private String clusterName;
        private String activeDc;
        private StartTimeAndInstances todo;
        private StartTimeAndInstances doing;
        private DynamicDelayPeriodTask task;

        public Aggregator(ClusterActiveDcKey key, ScheduledExecutorService scheduled, long markupWaitTimeoutInMills) {
            this.clusterName = key.getCluster();
            this.activeDc = key.getActiveDc();
            this.todo = new StartTimeAndInstances(markupWaitTimeoutInMills);
            this.doing = new StartTimeAndInstances(markupWaitTimeoutInMills);
            this.task = new DynamicDelayPeriodTask("Aggregator-" + clusterName + "-" + activeDc, new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    if (noInstance())
                        return;

                    handleInstances();
                }
            }, DefaultOuterClientAggregator.this::randomMill, DefaultOuterClientAggregator.this::checkInterval, scheduled);
        }

        public synchronized void add(HostPort hostPort) {
            this.todo.addInstance(hostPort);
        }

        public synchronized boolean markupWaitTimeout() {
            boolean timeout = doing.timeout();
            if (timeout) {
                logger.warn("[aggregator][{}:{}][markupWaitTimeout]startTime:{}", clusterName, activeDc, new Timestamp(doing.getWaitStartTime()));
            }
            return timeout;
        }

        public void start() throws Exception {
            task.start();
        }

        public synchronized boolean noInstance() {
            return todo.getInstances().isEmpty() && doing.getInstances().isEmpty();
        }

        public synchronized Set<HostPort> prepareInstances() {
            this.doing.merge(this.todo);
            this.todo.clear();
            return doing.getInstances();
        }

        public synchronized void done() {
            this.doing.clear();
        }

        public void handleInstances() {
            logger.info("[aggregator][{}:{}][handleInstances]", clusterName, activeDc);
            clusterOneThreadTaskExecutor.execute(clusterName, new AggregatorCheckAndSetTask(clusterName, activeDc, this));
        }

        @VisibleForTesting
        synchronized StartTimeAndInstances getDoing() {
            return doing;
        }

        @VisibleForTesting
        synchronized StartTimeAndInstances getTodo() {
            return todo;
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

    private Set<HostPortDcStatus> getNeedMarkInstances(String cluster, String activeDc, Set<HostPort> clusterHostPorts) throws Exception {
        logger.info("[aggregator][{}:{}][getNeedMarkInstances]{}", cluster, activeDc, clusterHostPorts);
        return aggregatorPullService.getNeedAdjustInstances(cluster, clusterHostPorts);
    }

    private void doMarkInstance(String cluster, String activeDc, Set<HostPortDcStatus> needMarkInstances) throws Exception {
        logger.info("[aggregator][{}:{}][doMarkInstance]{}", cluster, activeDc, needMarkInstances);
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

            Set<HostPort> doing = aggregator.prepareInstances();
            Set<HostPortDcStatus> instancesToUpdate;

            try {
                instancesToUpdate = getNeedMarkInstances(cluster, activeDc, doing);
            } catch (Throwable th) {
                logger.error("[aggregator][{}:{}][getNeedMarkInstances]", cluster, activeDc, th);
                aggregator.done();
                future().setFailure(th);
                return;
            }

            if (instancesToUpdate.isEmpty()) {
                logger.info("[aggregator][{}:{}][getNeedMarkInstances]empty, skip", cluster, activeDc);
                aggregator.done();
                future().setSuccess();
            } else {
                Set<HostPortDcStatus> markdownInstances = instancesToUpdate.stream().filter(hostPortDcStatus -> !hostPortDcStatus.isCanRead()).collect(Collectors.toSet());
                if (!markdownInstances.isEmpty() || aggregator.markupWaitTimeout() || dcInstancesAllUp(cluster, activeDc, instancesToUpdate)) {
                    for (HostPortDcStatus hostPortDcStatus : instancesToUpdate) {
                        for (HealthStateService stateService : healthStateServices) {
                            try {
                                stateService.updateLastMarkHandled(
                                        new HostPort(hostPortDcStatus.getHost(), hostPortDcStatus.getPort()),
                                        hostPortDcStatus.isCanRead());
                            } catch (Throwable th) {
                                logger.warn("[aggregator][{}:{}][updateLastMark][fail]", cluster, activeDc, th);
                            }
                        }
                    }

                    for (int i = 0; i < retry; i++) {
                        try {
                            logger.debug("[aggregator][{}:{}][doMarkInstance][begin]", cluster, activeDc);
                            doMarkInstance(cluster, activeDc, instancesToUpdate);
                            aggregator.done();
                            future().setSuccess();
                            logger.debug("[aggregator][{}:{}][doMarkInstance][end]", cluster, activeDc);
                            return;
                        } catch (Throwable th) {
                            logger.error("[aggregator][{}:{}][doMarkInstance][fail]", cluster, activeDc, th);
                        }
                    }
                    aggregator.done();
                    future().setFailure(new IllegalStateException("[aggregator][fail] "+ cluster));
                } else {
                    logger.info("[aggregator][{}:{}][allMarkup] continue wait", cluster, activeDc);
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

    boolean dcInstancesAllUp(String clusterName, String activeDc, Set<HostPortDcStatus> instances) {
        String allUpDc = aggregatorPullService.dcInstancesAllUp(clusterName, activeDc, instances);
        if (!Strings.isNullOrEmpty(allUpDc)) {
            logger.info("[aggregator][{}:{}][dcInstancesAllUp]{}", clusterName, activeDc, allUpDc);
            return true;
        } else {
            return false;
        }
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
