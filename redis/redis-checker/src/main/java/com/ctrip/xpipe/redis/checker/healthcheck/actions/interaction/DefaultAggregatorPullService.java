package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.OuterClientService.HostPortDcStatus;
import com.ctrip.xpipe.api.migration.OuterClientService.MarkInstanceRequest;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Component
public class DefaultAggregatorPullService implements AggregatorPullService{

    @Autowired
    private RemoteCheckerManager remoteCheckerManager;
    @Autowired
    private AlertManager alertManager;
    @Autowired
    private CheckerConfig checkerConfig;
    @Autowired
    private MetaCache metaCache;
    @Autowired
    private DefaultDelayPingActionCollector defaultDelayPingActionCollector;
    @Autowired
    private DefaultPsubPingActionCollector defaultPsubPingActionCollector;

    private OuterClientService outerClientService = OuterClientService.DEFAULT;
    private static final Logger logger =  LoggerFactory.getLogger(DefaultAggregatorPullService.class);
    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();
    private Executor executors;

    @PostConstruct
    public void postConstruct() {
        this.executors = new ThreadPoolExecutor(100, 100, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(256), XpipeThreadFactory.create("DefaultAggregatorPullService"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public Set<HostPortDcStatus> getNeedAdjustInstances(String cluster, Set<HostPort> instances) throws Exception {
        Set<HostPortDcStatus> instanceNeedAdjust = new HashSet<>();

        QueryXPipeInstanceStatusCmd queryXPipeInstanceStatusCmd = new QueryXPipeInstanceStatusCmd(cluster, instances);
        QueryOuterClintInstanceStatusCmd queryOuterClintInstanceStatusCmd = new QueryOuterClintInstanceStatusCmd(cluster, instances);

        CommandFuture<XPipeInstanceHealthHolder> xpipeQueryFuture = queryXPipeInstanceStatusCmd.execute(executors);
        CommandFuture<Map<HostPort, Boolean>> outerClientQueryFuture = queryOuterClintInstanceStatusCmd.execute(executors);

        XPipeInstanceHealthHolder xpipeInstanceHealthHolder = xpipeQueryFuture.get();
        Map<HostPort, Boolean> outerClientAllHealthStatus = outerClientQueryFuture.get();
        Map<HostPort, Boolean> xpipeAllHealthStatus = xpipeInstanceHealthHolder.getAllHealthStatus(checkerConfig.getQuorum());
        Map<HostPort, Boolean> lastMarks = xpipeInstanceHealthHolder.getOtherCheckerLastMark();

        for (Map.Entry<HostPort, Boolean> entry : xpipeAllHealthStatus.entrySet()) {
            HostPort instance = entry.getKey();
            Boolean expectMark = entry.getValue();
            if (null == expectMark) {
                logger.info("[getNeedAdjustInstances][unknown host] {}", instance);
                continue;
            }

            if (!outerClientAllHealthStatus.containsKey(instance)
                    || !expectMark.equals(outerClientAllHealthStatus.get(instance))) {
                if (lastMarks.containsKey(instance) && expectMark.equals(lastMarks.get(instance))) {
                    logger.info("[getNeedAdjustInstances][otherCheckerMark][{}-{}] skip", instance, expectMark);
                    continue;
                }
                instanceNeedAdjust.add(new HostPortDcStatus(instance.getHost(), instance.getPort(),
                        metaCache.getDc(instance), expectMark));
            }
        }
        return instanceNeedAdjust;
    }

    @Override
    public void doMarkInstances(String clusterName, String activeDc, Set<HostPortDcStatus> instances) throws OuterClientException {
        setSuspectIfNeeded(instances);
        alertMarkInstance(clusterName, instances);
        Map<String, Integer> dcInstancesCnt = metaCache.getClusterCntMap(clusterName);
        MarkInstanceRequest markInstanceRequest = new MarkInstanceRequest(instances, clusterName, activeDc, dcInstancesCnt);
        outerClientService.batchMarkInstance(markInstanceRequest);
    }

    @Override
    public void doMarkInstancesIfNoModifyFor(String clusterName, String activeDc, Set<HostPortDcStatus> instances, long seconds) throws OuterClientException {
        setSuspectIfNeeded(instances);
        alertMarkInstance(clusterName, instances);
        Map<String, Integer> dcInstancesCnt = metaCache.getClusterCntMap(clusterName);
        MarkInstanceRequest markInstanceRequest = new MarkInstanceRequest(instances, clusterName, activeDc, dcInstancesCnt, (int) seconds);
        outerClientService.batchMarkInstance(markInstanceRequest);
    }

    @Override
    public String dcInstancesAllUp(String clusterName, String activeDc, Set<HostPortDcStatus> instancesToMarkup) {
        Set<String> relatedDcs = new HashSet<>();
        instancesToMarkup.forEach(hostPortDcStatus -> relatedDcs.add(metaCache.getDc(new HostPort(hostPortDcStatus.getHost(), hostPortDcStatus.getPort()))));

        Map<String, List<RedisMeta>> dcInstances = metaCache.getAllInstance(clusterName);
        Map<HostPort, HealthStatusDesc> allStatus;
        if (crossRegion(relatedDcs, activeDc)) {
            allStatus = defaultPsubPingActionCollector.getAllHealthStatus();
        } else {
            allStatus = defaultDelayPingActionCollector.getAllHealthStatus();
        }

        for (String dc : relatedDcs) {
            List<HostPort> allDcInstances = dcInstances.get(dc).stream().map(redisMeta -> new HostPort(redisMeta.getIp(), redisMeta.getPort())).collect(Collectors.toList());
            if (allInstancesUp(allDcInstances, allStatus))
                return dc;
        }
        return null;
    }

    boolean crossRegion(Set<String> relatedDcs, String activeDc) {
        for (String relatedDc : relatedDcs) {
            if (metaCache.isCrossRegion(activeDc, relatedDc))
                return true;
        }
        return false;
    }

    boolean allInstancesUp(List<HostPort> instances, Map<HostPort, HealthStatusDesc> allStatus) {
        for (HostPort hostPort : instances) {
            HealthStatusDesc healthStatusDesc = allStatus.get(hostPort);
            if (healthStatusDesc == null) {
                continue;
            }
            HEALTH_STATE healthState = healthStatusDesc.getState();
            if (!healthState.shouldNotifyMarkup())
                return false;
        }
        return true;
    }

    private void alertMarkInstance(String clusterName, Set<HostPortDcStatus> instances) {
        if (instances.isEmpty()) return;

        try {
            for (HostPortDcStatus instance : instances) {
                if (instance.isSuspect())
                    continue;

                if (instance.isCanRead()) {
                    alertManager.alert(clusterName, null,
                            new HostPort(instance.getHost(), instance.getPort()), ALERT_TYPE.MARK_INSTANCE_UP, "Mark Instance Up");
                } else {
                    alertManager.alert(clusterName, null,
                            new HostPort(instance.getHost(), instance.getPort()), ALERT_TYPE.MARK_INSTANCE_DOWN, "Mark Instance Down");
                }
            }
        } catch (Throwable th) {
            logger.info("[alertMarkInstance][{}] fail", clusterName, th);
        }
    }

    private void setSuspectIfNeeded(Set<HostPortDcStatus> instances) {
        instances.forEach(instance -> {
            if (!instance.isCanRead() && metaCache.isCrossRegion(currentDc, instance.getDc())) {
                instance.setCanRead(true);
                instance.setSuspect(true);
            }
        });
    }

    protected class QueryXPipeInstanceStatusCmd extends AbstractCommand<XPipeInstanceHealthHolder> {

        private String cluster;

        private Set<HostPort> instances;

        public QueryXPipeInstanceStatusCmd(String cluster, Set<HostPort> instances) {
            this.cluster = cluster;
            this.instances = instances;
        }

        @Override
        protected void doExecute() throws Throwable {
            XPipeInstanceHealthHolder xpipeInstanceHealthHolder = new XPipeInstanceHealthHolder();
            for (CheckerService checkerService : remoteCheckerManager.getAllCheckerServices()) {
                xpipeInstanceHealthHolder.add(checkerService.getAllClusterInstanceHealthStatus(instances));
            }
            future().setSuccess(xpipeInstanceHealthHolder);
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return "QueryXPipeInstanceStatusCmd";
        }
    }

    protected class QueryOuterClintInstanceStatusCmd extends AbstractCommand<Map<HostPort, Boolean>> {

        private String cluster;

        private Set<HostPort> instances;

        public QueryOuterClintInstanceStatusCmd(String cluster, Set<HostPort> instances) {
            this.cluster = cluster;
            this.instances = instances;
        }

        @Override
        protected void doExecute() throws Throwable {
            Map<HostPort, OuterClientService.OutClientInstanceStatus> outClientInstanceStatus = outerClientService.batchQueryInstanceStatus(cluster, instances);
            Map<HostPort, Boolean> localInstanceStatus = new HashMap<>();
            for (Map.Entry<HostPort, OuterClientService.OutClientInstanceStatus> entry : outClientInstanceStatus.entrySet()) {
                OuterClientService.OutClientInstanceStatus outStatus = entry.getValue();
                if (outStatus.isSuspect() && metaCache.isCrossRegion(currentDc, entry.getValue().getEnv())) {
                    localInstanceStatus.put(entry.getKey(), false);
                } else {
                    localInstanceStatus.put(entry.getKey(), outStatus.isCanRead());
                }
            }
            future().setSuccess(localInstanceStatus);
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return "[QueryOuterClintInstanceStatusCmd]" + cluster;
        }
    }

    @VisibleForTesting
    protected void setExecutors(Executor executors) {
        this.executors = executors;
    }

    @VisibleForTesting
    protected void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

}
