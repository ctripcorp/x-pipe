package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
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

    private HashMap<String, Set<HostPort>> clusterAggregators = new HashMap<>();

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    private AggregatorPullService aggregatorPullService;

    private Random rand = new Random();

    private KeyedOneThreadTaskExecutor<String> clusterOneThreadTaskExecutor;

    private static final Logger logger =  LoggerFactory.getLogger(DefaultOuterClientAggregator.class);

    @PostConstruct
    public void postConstruct() {
        clusterOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<>(executors);
    }

    @Override
    public void markInstance(ClusterShardHostPort info) {
        Set<HostPort> aggregator = MapUtils.getOrCreate(clusterAggregators, info.getClusterName(), HashSet::new);
        synchronized (aggregator) {
            boolean emptyBeforeAdd = aggregator.isEmpty();
            if (aggregator.add(info.getHostPort()) && emptyBeforeAdd) {
                scheduled.schedule(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        Set<HostPort> batch;
                        Set<HostPort> innerAggregator = clusterAggregators.get(info.getClusterName());
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
                        handleInstances(info.getClusterName(), batch);
                    }
                }, randomMill(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void handleInstances(String clusterName, Set<HostPort> instances) {
        logger.info("[handleInstances][{}] {}", clusterName, instances);
        clusterOneThreadTaskExecutor.execute(clusterName, new AggregatorCheckAndSetTask(clusterName, instances));
    }

    @VisibleForTesting
    public int randomMill() {
        int delayBase = Math.max(checkerConfig.getMarkInstanceBaseDelayMilli(), 0);
        int delayMax = Math.max(checkerConfig.getMarkInstanceMaxDelayMilli(), delayBase);
        return delayBase + rand.nextInt(delayMax - delayBase);
    }

    private Set<HostPortDcStatus> getNeedMarkInstances(String cluster, Set<HostPort> clusterHostPorts) throws Exception {
        logger.info("[getNeedMarkInstances][{}]", cluster);
        return aggregatorPullService.getNeedAdjustInstances(cluster, clusterHostPorts);
    }

    private void doMarkInstance(String cluster, Set<HostPortDcStatus> needMarkInstances) throws Exception {
        logger.info("[doMarkInstance][{}]", cluster);
        aggregatorPullService.doMarkInstances(cluster, needMarkInstances);
    }

    public class AggregatorCheckAndSetTask extends AbstractCommand<Void> {

        private int retry;

        private String cluster;

        private Set<HostPort> instances;

        public AggregatorCheckAndSetTask(String cluster, Set<HostPort> instances) {
            this(cluster, instances, 3);
        }

        public AggregatorCheckAndSetTask(String cluster, Set<HostPort> instances, int retry){
            this.cluster = cluster;
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
                for(int i=0; i < retry ;i++){
                    try{
                        logger.debug("[aggregator][begin] {}", cluster);
                        doMarkInstance(cluster, instancesToUpdate);
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

}
