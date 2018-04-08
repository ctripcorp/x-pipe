package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.*;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaBuilder;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Apr 02, 2018
 */
@Service
public class AdvancedDcMetaService implements DcMetaService {

    private static final Logger logger = LoggerFactory.getLogger(AdvancedDcMetaService.class);

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private SentinelService sentinelService;

    @Autowired
    private KeepercontainerService keepercontainerService;

    @Autowired
    private SentinelMetaService sentinelMetaService;

    @Autowired
    private KeepercontainerMetaService keepercontainerMetaService;

    @Autowired
    private RedisMetaService redisMetaService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private ClusterMetaService clusterMetaService;

    @Resource(name=SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private ExecutorService executors;

    private RetryCommandFactory factory;

    @PostConstruct
    public void initService() {
        executors = DefaultExecutorFactory.createAllowCoreTimeout("OptimizedDcMetaService", OsUtils.defaultMaxCoreThreadCount())
                .createExecutorService();
        int retryTimes = 3, retryDelayMilli = 5;
        factory = new DefaultRetryCommandFactory(retryTimes, new RetryDelay(retryDelayMilli), scheduled);
    }

    @Override
    public DcMeta getDcMeta(String dcName) {
        DcTbl dcTbl = dcService.find(dcName);

        DcMeta dcMeta = new DcMeta().setId(dcName).setLastModifiedTime(dcTbl.getDcLastModifiedTime());

        ParallelCommandChain chain = new ParallelCommandChain(executors);
        chain.add(retry3TimesUntilSuccess(new GetAllSentinelCommand(dcMeta)));
        chain.add(retry3TimesUntilSuccess(new GetAllKeeperContainerCommand(dcMeta)));

        DcMetaBuilder builder = new DcMetaBuilder(dcMeta, dcTbl.getId(), executors, redisMetaService, dcClusterService,
                clusterMetaService, dcClusterShardService, dcService, factory);
        chain.add(retry3TimesUntilSuccess(builder));

        try {
            chain.execute().get();
        } catch (Exception e) {
            logger.error("[queryDcMeta] ", e);
        }

        return dcMeta;

    }

    @VisibleForTesting
    protected <T> Command<T> retry3TimesUntilSuccess(Command<T> command) {
        return factory.createRetryCommand(command);
    }

    class GetAllSentinelCommand extends AbstractCommand<Void> {

        private DcMeta dcMeta;

        public GetAllSentinelCommand(DcMeta dcMeta) {
            this.dcMeta = dcMeta;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                List<SetinelTbl> sentinels = sentinelService.findAllByDcName(dcMeta.getId());
                sentinels.forEach(sentinel -> dcMeta
                        .addSentinel(sentinelMetaService.encodeSetinelMeta(sentinel, dcMeta)));
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcMeta.getSentinels().clear();
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }

    class GetAllKeeperContainerCommand extends AbstractCommand<Void> {

        private DcMeta dcMeta;

        public GetAllKeeperContainerCommand(DcMeta dcMeta) {
            this.dcMeta = dcMeta;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                List<KeepercontainerTbl> keepercontainers = keepercontainerService.findAllByDcName(dcMeta.getId());
                keepercontainers.forEach(keeperContainer -> dcMeta.addKeeperContainer(
                        keepercontainerMetaService.encodeKeepercontainerMeta(keeperContainer, dcMeta)));
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {
            dcMeta.getKeeperContainers().clear();
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }

    /**-----------------------Visible for Test-----------------------------------------*/
    public AdvancedDcMetaService setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

    public AdvancedDcMetaService setExecutors(ExecutorService executors) {
        this.executors = executors;
        return this;
    }

    public AdvancedDcMetaService setFactory(RetryCommandFactory factory) {
        this.factory = factory;
        return this;
    }
}
