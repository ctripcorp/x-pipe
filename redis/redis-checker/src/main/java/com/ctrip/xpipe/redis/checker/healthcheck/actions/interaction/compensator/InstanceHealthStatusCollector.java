package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.OutClientInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author lishanglin
 * date 2022/7/21
 */
@Component
public class InstanceHealthStatusCollector {

    private RemoteCheckerManager remoteCheckerManager;

    private OuterClientCache outerClientCache;

    private ExecutorService executors;

    private Logger logger = LoggerFactory.getLogger(InstanceHealthStatusCollector.class);

    @Autowired
    public InstanceHealthStatusCollector(RemoteCheckerManager remoteCheckerManager, OuterClientCache outerClientCache,
                                         @Qualifier(GLOBAL_EXECUTOR) ExecutorService executors) {
        this.remoteCheckerManager = remoteCheckerManager;
        this.outerClientCache = outerClientCache;
        this.executors = executors;
    }

    public Pair<XPipeInstanceHealthHolder, OutClientInstanceHealthHolder> collect() throws ExecutionException, InterruptedException, TimeoutException {
        return collect(false);
    }

    public Pair<XPipeInstanceHealthHolder, OutClientInstanceHealthHolder> collect(boolean isCrossRegion)
            throws InterruptedException, ExecutionException, TimeoutException {
        XPipeInstanceHealthHolder xpipeInstanceHealthHolder = new XPipeInstanceHealthHolder();
        OutClientInstanceHealthHolder outClientInstanceHealthHolder = new OutClientInstanceHealthHolder();

        ParallelCommandChain commandChain = new ParallelCommandChain(executors);
        commandChain.add(new GetAllOutClientInstanceStatusCmd(outClientInstanceHealthHolder, isCrossRegion));
        remoteCheckerManager.getAllCheckerServices().forEach(checkerService -> {
            commandChain.add(new GetRemoteCheckResultCmd(checkerService, xpipeInstanceHealthHolder, isCrossRegion));
        });
        commandChain.execute().get(5, TimeUnit.SECONDS);

        return new Pair<>(xpipeInstanceHealthHolder, outClientInstanceHealthHolder);
    }

    public XPipeInstanceHealthHolder collectXPipeInstanceHealth(HostPort hostPort, boolean isCrossRegion)
            throws InterruptedException, ExecutionException, TimeoutException {
        ParallelCommandChain commandChain = new ParallelCommandChain(executors);
        XPipeInstanceHealthHolder xpipeInstanceHealthHolder = new XPipeInstanceHealthHolder();
        remoteCheckerManager.getAllCheckerServices().forEach(checkerService -> {
            commandChain.add(new GetRemoteHealthStateCmd(hostPort, checkerService, xpipeInstanceHealthHolder, isCrossRegion));
        });
        commandChain.execute().get(5, TimeUnit.SECONDS);

        return xpipeInstanceHealthHolder;
    }

    private class GetRemoteCheckResultCmd extends AbstractCommand<Void> {

        private CheckerService checkerService;

        private XPipeInstanceHealthHolder resultHolder;

        private boolean isCrossRegion;

        public GetRemoteCheckResultCmd(CheckerService checkerService, XPipeInstanceHealthHolder xpipeInstanceHealthHolder, boolean isCrossRegion) {
            this.checkerService = checkerService;
            this.resultHolder = xpipeInstanceHealthHolder;
            this.isCrossRegion = isCrossRegion;
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                if (isCrossRegion) {
                    resultHolder.add(checkerService.getAllInstanceCrossRegionHealthStatus());
                } else {
                    resultHolder.add(checkerService.getAllInstanceHealthStatus());
                }
            } catch (RestClientException restException) {
                logger.info("[doExecute][rest fail] {}", restException.getMessage());
            } catch (Throwable th) {
                logger.info("[doExecute][fail]", th);
            }

            future().setSuccess();
        }

        @Override
        protected void doReset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return "GetRemoteCheckResultCmd";
        }
    }

    private class GetAllOutClientInstanceStatusCmd extends AbstractCommand<Void> {

        private OutClientInstanceHealthHolder resultHolder;

        private boolean isCrossRegion;

        public GetAllOutClientInstanceStatusCmd(OutClientInstanceHealthHolder outClientInstanceHealthHolder, boolean isCrossRegion) {
            this.resultHolder = outClientInstanceHealthHolder;
            this.isCrossRegion = isCrossRegion;
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                if (isCrossRegion) {
                    resultHolder.addClusters(
                            outerClientCache.getAllCurrentDcClusters(FoundationService.DEFAULT.getDataCenter()));
                } else {
                    resultHolder.addClusters(
                            outerClientCache.getAllDcClusters(FoundationService.DEFAULT.getDataCenter()));
                }
            } catch (Throwable th) {
                logger.info("[doExecute][fail]", th);
            }

            future().setSuccess();
        }

        @Override
        protected void doReset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return "GetOutClientInstanceStatusCmd";
        }
    }

    private class GetRemoteHealthStateCmd extends AbstractCommand<Void> {

        private HostPort hostPort;

        private CheckerService checkerService;

        private XPipeInstanceHealthHolder resultHolder;

        private boolean isCrossRegion;

        public GetRemoteHealthStateCmd(HostPort hostPort, CheckerService checkerService, XPipeInstanceHealthHolder xpipeInstanceHealthHolder, boolean isCrossRegion) {
            this.hostPort = hostPort;
            this.checkerService = checkerService;
            this.resultHolder = xpipeInstanceHealthHolder;
            this.isCrossRegion = isCrossRegion;
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                HEALTH_STATE status;
                if (isCrossRegion) {
                    status = checkerService.getCrossRegionInstanceStatus(hostPort.getHost(), hostPort.getPort());
                } else {
                    status = checkerService.getInstanceStatus(hostPort.getHost(), hostPort.getPort());
                }
                resultHolder.add(new HealthStatusDesc(hostPort, status));
            } catch (RestClientException restException) {
                logger.info("[doExecute][rest fail] {}", restException.getMessage());
            } catch (Throwable th) {
                logger.info("[doExecute][fail]", th);
            }

            future().setSuccess();
        }

        @Override
        protected void doReset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return "GetRemoteHealthStateCmd";
        }

    }

}
