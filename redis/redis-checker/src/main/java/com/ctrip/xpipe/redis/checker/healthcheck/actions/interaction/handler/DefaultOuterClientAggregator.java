package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.concurrent.AggregatorStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
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

    private final ConcurrentHashMap<String, Set<HostPort>> registry = new ConcurrentHashMap<>();
    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;
    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;
    @Autowired
    private CheckerConfig checkerConfig;
    @Autowired
    private AggregatorPullService aggregatorPullService;
    private final Random rand = new Random();
    private AggregatorStateSetterManager<ClusterHostPorts, Set<HostPortDcStatus>> finalStateSetterManager;
    private static final Logger logger =  LoggerFactory.getLogger(DefaultOuterClientAggregator.class);
    @PostConstruct
    public void postConstruct() {
        finalStateSetterManager = new AggregatorStateSetterManager<>(executors, this::getNeedMarkInstances, this::doMarkInstance);
    }

    @Override
    public void markInstance(ClusterShardHostPort info) {
        synchronized (info.getClusterName().intern()) {
            Set<HostPort> instances = registry.computeIfAbsent(info.getClusterName(), k -> new HashSet<>());
            if (instances.add(info.getHostPort()) && instances.size() == 1) {
                scheduled.schedule(() -> {
                    synchronized (info.getClusterName().intern()) {
                        Set<HostPort> instancesToRemove = registry.remove(info.getClusterName());
                        if (instancesToRemove != null) {
                            handleInstances(info.getClusterName(), instancesToRemove);
                        }
                    }
                }, randomMill(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void handleInstances(String clusterName, Set<HostPort> instances) {
        logger.info("[DefaultOuterClientAggregator][handleInstances]cluster:{}, instances:{}", clusterName, instances);
        finalStateSetterManager.set(new ClusterHostPorts(clusterName, instances));
    }

    @VisibleForTesting
    public long randomMill () {
        double randomNum = checkerConfig.getInstancePullIntervalSeconds() + rand.nextDouble() * checkerConfig.getInstancePullRandomSeconds();
        return (long) (randomNum * 1000);
    }

    private Set<HostPortDcStatus> getNeedMarkInstances(ClusterHostPorts clusterHostPorts) {
        try {
            logger.info("[DefaultOuterClientAggregator][getNeedMarkInstances]clusterHostPorts:{}", clusterHostPorts);
            return aggregatorPullService.getNeedAdjustInstances(clusterHostPorts.getHostPorts());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void doMarkInstance(ClusterHostPorts clusterHostPorts, Set<HostPortDcStatus> needMarkInstances) {
        try {
            logger.info("[DefaultOuterClientAggregator][handleInstances]clusterHostPorts:{}, needMarkInstances:{}", clusterHostPorts, needMarkInstances);
            aggregatorPullService.doMarkInstances(clusterHostPorts.getClusterName(), needMarkInstances);
        } catch (Exception e) {
            throw new IllegalStateException("set error:" + needMarkInstances, e);
        }
    }

    private static class ClusterHostPorts {
        private String clusterName;
        private Set<HostPort> hostPorts;

        public ClusterHostPorts(String clusterName, Set<HostPort> hostPorts) {
            this.clusterName = clusterName;
            this.hostPorts = hostPorts;
        }

        public String getClusterName() {
            return clusterName;
        }

        public Set<HostPort> getHostPorts() {
            return hostPorts;
        }

        @Override
        public String toString() {
            return "ClusterHostPorts{" +
                    "clusterName='" + clusterName + '\'' +
                    ", hostPorts=" + hostPorts +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ClusterHostPorts)) return false;
            ClusterHostPorts that = (ClusterHostPorts) o;
            return Objects.equals(getClusterName(), that.getClusterName()) && Objects.equals(getHostPorts(), that.getHostPorts());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClusterName(), getHostPorts());
        }
    }

    @VisibleForTesting
    public void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

}
