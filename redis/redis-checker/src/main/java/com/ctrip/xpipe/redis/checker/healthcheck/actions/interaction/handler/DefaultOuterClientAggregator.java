package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

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
                        prepareInstances();
                        if (doing.isEmpty())
                            return;

                        handleInstances();
                    }
                }, randomMill(), checkInterval(), TimeUnit.MILLISECONDS);
        }

        public synchronized void prepareInstances() {
            doing.addAll(todo);
            todo.clear();
        }

        public synchronized void done() {
            this.doing.clear();
            this.waitStartTime = 0;
        }

        public void handleInstances() {
            logger.info("[handleInstances][{}:{}] {}", clusterName, activeDc, doing);
            clusterOneThreadTaskExecutor.execute(clusterName, new AggregatorCheckAndSetTask(clusterName, activeDc, this));
        }

        public synchronized Set<HostPort> getDoing() {
            return doing;
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getActiveDc() {
            return activeDc;
        }

    }

    @VisibleForTesting
    public int randomMill() {
        int delayBase = Math.max(checkerConfig.getMarkInstanceBaseDelayMilli(),
                checkerConfig.getRedisReplicationHealthCheckInterval() + checkerConfig.getCheckerMetaRefreshIntervalMilli());
        int delayMax = Math.max(checkerConfig.getMarkdownInstanceMaxDelayMilli(), delayBase + DELTA);
        return delayBase + rand.nextInt(delayMax - delayBase);
    }

    public int checkInterval() {
        int delayBase = Math.max(checkerConfig.getMarkInstanceBaseDelayMilli(),
                checkerConfig.getRedisReplicationHealthCheckInterval() + checkerConfig.getCheckerMetaRefreshIntervalMilli());
        return Math.max(checkerConfig.getMarkdownInstanceMaxDelayMilli(), delayBase + DELTA);
    }

    private Set<HostPortDcStatus> getNeedMarkInstances(String cluster, Set<HostPort> clusterHostPorts) throws Exception {
        logger.info("[getNeedMarkInstances][{}]", cluster);
        return aggregatorPullService.getNeedAdjustInstances(cluster, clusterHostPorts);
    }

    private void doMarkInstance(String cluster, String activeDc, Set<HostPortDcStatus> needMarkInstances) throws Exception {
        logger.info("[doMarkInstance][{}]", cluster);
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

            Set<HostPortDcStatus> instancesToUpdate;

            try {
                instancesToUpdate = getNeedMarkInstances(cluster, aggregator.getDoing());
            } catch (Throwable th) {
                logger.info("[aggregator]][getFail[{}]", cluster, th);
                future().setFailure(th);
                //todo: aggregator done?
                return;
            }

            if (instancesToUpdate.isEmpty()) {
                logger.info("[aggregator][getEmpty] skip");
                aggregator.done();
                future().setSuccess();
            } else {
                Set<HostPortDcStatus> markdownInstances = instancesToUpdate.stream().filter(hostPortDcStatus -> !hostPortDcStatus.isCanRead()).collect(Collectors.toSet());
                if (!markdownInstances.isEmpty() || aggregator.markupWaitTimeout() || dcInstancesAllUp(aggregator)) {
                    for (HostPortDcStatus hostPortDcStatus : instancesToUpdate) {
                        for (HealthStateService stateService : healthStateServices) {
                            try {
                                stateService.updateLastMarkHandled(
                                        new HostPort(hostPortDcStatus.getHost(), hostPortDcStatus.getPort()),
                                        hostPortDcStatus.isCanRead());
                            } catch (Throwable th) {
                                logger.info("[aggregator][updateLastMark][fail]", th);
                            }
                        }
                    }

                    for (int i = 0; i < retry; i++) {
                        try {
                            logger.debug("[aggregator][begin] {}", cluster);
                            doMarkInstance(cluster, activeDc, instancesToUpdate);
                            future().setSuccess();
                            logger.debug("[aggregator][end] {}", cluster);
                            return;
                        } catch (Throwable th) {
                            logger.error("[aggregator][setFail] " + cluster, th);
                        }
                    }
                    aggregator.done();
                    future().setSuccess();
                } else {
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

    public boolean dcInstancesAllUp(Aggregator aggregator) {
        return aggregatorPullService.dcInstancesAllUp(aggregator.clusterName, aggregator.getDoing());
    }

    @VisibleForTesting
    public void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @VisibleForTesting
    public void setHealthStateServices(List<HealthStateService> healthStateServices) {
        this.healthStateServices = healthStateServices;
    }

}
