package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class DefaultOuterClientAggregator implements OuterClientAggregator{

    private HashMap<ClusterActiveDcKey, Set<HostPort>> clusterAggregators = new HashMap<>();

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

    private static final Logger logger =  LoggerFactory.getLogger(DefaultOuterClientAggregator.class);

    private static final int DELTA = 500;

    @PostConstruct
    public void postConstruct() {
        clusterOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<>(executors);
    }

    @Override
    public void markInstance(ClusterShardHostPort info) {
        ClusterActiveDcKey key = new ClusterActiveDcKey(info.getClusterName(), info.getActiveDc());
        Set<HostPort> aggregator = MapUtils.getOrCreate(clusterAggregators, key, HashSet::new);
        synchronized (aggregator) {
            boolean emptyBeforeAdd = aggregator.isEmpty();
            if (aggregator.add(info.getHostPort()) && emptyBeforeAdd) {
                scheduled.schedule(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        Set<HostPort> batch;
                        Set<HostPort> innerAggregator = clusterAggregators.get(key);
                        if (null == innerAggregator) {
                            logger.warn("[markInstance][aggregate][unexpected null aggregator] {}", info.getClusterName());
                            return;
                        }

                        synchronized (innerAggregator) {
                            if (innerAggregator.isEmpty()) {
                                logger.warn("[markInstance][aggregate][unexpected empty aggregator] {}", info.getClusterName());
                                return;
                            }
                            batch = new HashSet<>(innerAggregator);
                            innerAggregator.clear();
                        }
                        handleInstances(key.cluster, key.activeDc, batch);
                    }
                }, randomMill(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void handleInstances(String cluster, String activeDc, Set<HostPort> instances) {
        logger.info("[handleInstances][{}:{}] {}", cluster, activeDc, instances);
        clusterOneThreadTaskExecutor.execute(cluster, new AggregatorCheckAndSetTask(cluster, activeDc, instances));
    }

    @VisibleForTesting
    public int randomMill() {
        int delayBase = Math.max(checkerConfig.getMarkInstanceBaseDelayMilli(),
                checkerConfig.getCheckerMetaRefreshIntervalMilli() + checkerConfig.getRedisReplicationHealthCheckInterval() + DELTA);
        int delayMax = Math.max(checkerConfig.getMarkInstanceMaxDelayMilli(), delayBase + DELTA);
        return delayBase + rand.nextInt(delayMax - delayBase);
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

        private Set<HostPort> instances;

        public AggregatorCheckAndSetTask(String cluster, String activeDc, Set<HostPort> instances) {
            this(cluster, activeDc, instances, 3);
        }

        public AggregatorCheckAndSetTask(String cluster, String activeDc, Set<HostPort> instances, int retry){
            this.cluster = cluster;
            this.activeDc = activeDc;
            this.instances = instances;
            this.retry = retry;
        }

        @Override
        protected void doExecute() throws Exception {

            Set<HostPortDcStatus> instancesToUpdate;

            try {
                instancesToUpdate = getNeedMarkInstances(cluster, instances);
            } catch (Throwable th) {
                logger.info("[aggregator]][getFail[{}]", cluster, th);
                future().setFailure(th);
                return;
            }

            if (instancesToUpdate.isEmpty()) {
                logger.info("[aggregator][getEmpty] skip");
                future().setSuccess();
            } else {
                for (HostPortDcStatus hostPortDcStatus: instancesToUpdate) {
                    for (HealthStateService stateService: healthStateServices) {
                        try {
                            stateService.updateLastMarkHandled(
                                    new HostPort(hostPortDcStatus.getHost(), hostPortDcStatus.getPort()),
                                    hostPortDcStatus.isCanRead());
                        } catch (Throwable th) {
                            logger.info("[aggregator][updateLastMark][fail]", th);
                        }
                    }
                }

                for(int i=0; i < retry ;i++){
                    try{
                        logger.debug("[aggregator][begin] {}", cluster);
                        doMarkInstance(cluster, activeDc, instancesToUpdate);
                        future().setSuccess();
                        logger.debug("[aggregator][end] {}", cluster);
                        return;
                    } catch (Throwable th){
                        logger.error("[aggregator][setFail] " + cluster, th);
                    }
                }
                future().setFailure(new IllegalStateException("[aggregator][fail]" + cluster));
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

    @VisibleForTesting
    public void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @VisibleForTesting
    public void setHealthStateServices(List<HealthStateService> healthStateServices) {
        this.healthStateServices = healthStateServices;
    }

    private static class ClusterActiveDcKey {

        private String cluster;

        private String activeDc;

        public ClusterActiveDcKey(String cluster, String activeDc) {
            this.cluster = cluster;
            this.activeDc = activeDc;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClusterActiveDcKey that = (ClusterActiveDcKey) o;
            return Objects.equals(cluster, that.cluster) &&
                    Objects.equals(activeDc, that.activeDc);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, activeDc);
        }

        @Override
        public String toString() {
            return cluster + ":" + activeDc;
        }
    }

}
