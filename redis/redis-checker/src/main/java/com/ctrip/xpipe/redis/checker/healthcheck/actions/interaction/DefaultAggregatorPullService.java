package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;


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
    private OuterClientService outerClientService = OuterClientService.DEFAULT;
    private static final Logger logger =  LoggerFactory.getLogger(DefaultAggregatorPullService.class);

    private ExecutorService executors;

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

        CommandFuture<Map<HostPort, Boolean>> xpipeQueryFuture = queryXPipeInstanceStatusCmd.execute(executors);
        CommandFuture<Map<HostPort, Boolean>> outerClientQueryFuture = queryOuterClintInstanceStatusCmd.execute(executors);

        Map<HostPort, Boolean> xpipeAllHealthStatus = xpipeQueryFuture.get();
        Map<HostPort, Boolean> outerClientAllHealthStatus = outerClientQueryFuture.get();

        for (Map.Entry<HostPort, Boolean> entry : xpipeAllHealthStatus.entrySet()) {
            if (!outerClientAllHealthStatus.containsKey(entry.getKey())
                    || !entry.getValue().equals(outerClientAllHealthStatus.get(entry.getKey()))) {
                instanceNeedAdjust.add(
                        new HostPortDcStatus(entry.getKey().getHost(), entry.getKey().getPort(), metaCache.getDc(entry.getKey()), entry.getValue()));
            }
        }
        return instanceNeedAdjust;
    }

    @Override
    public void doMarkInstances(String clusterName, Set<HostPortDcStatus> instances) throws OuterClientException {
        alertMarkInstance(clusterName, instances);
        MarkInstanceRequest markInstanceRequest = new MarkInstanceRequest(instances, clusterName, metaCache.getActiveDc(clusterName));
        outerClientService.batchMarkInstance(markInstanceRequest);
    }

    private void alertMarkInstance(String clusterName, Set<HostPortDcStatus> instances) {
        if (instances.isEmpty()) return;

        try {
            for (HostPortDcStatus instance : instances) {
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

    private class QueryXPipeInstanceStatusCmd extends AbstractCommand<Map<HostPort, Boolean>> {

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
            future().setSuccess(xpipeInstanceHealthHolder.getAllHealthStatus(checkerConfig.getQuorum()));
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return "QueryXPipeInstanceStatusCmd";
        }
    }

    private class QueryOuterClintInstanceStatusCmd extends AbstractCommand<Map<HostPort, Boolean>> {

        private String cluster;

        private Set<HostPort> instances;

        public QueryOuterClintInstanceStatusCmd(String cluster, Set<HostPort> instances) {
            this.cluster = cluster;
            this.instances = instances;
        }

        @Override
        protected void doExecute() throws Throwable {
            future().setSuccess(outerClientService.batchQueryInstanceStatus(cluster, instances));
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return "[QueryOuterClintInstanceStatusCmd]" + cluster;
        }
    }

}