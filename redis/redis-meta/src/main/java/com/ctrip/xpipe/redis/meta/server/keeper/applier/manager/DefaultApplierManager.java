package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.CausalChain;
import com.ctrip.xpipe.command.CausalCommand;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.ApplierState;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.exception.ApplierStateInCorrectException;
import com.ctrip.xpipe.redis.meta.server.job.ApplierStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierManager;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierStateController;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author ayq
 * <p>
 * 2022/3/31 21:15
 */
@Component
public class DefaultApplierManager extends AbstractCurrentMetaObserver implements ApplierManager, TopElement {

    private int deadApplierCheckIntervalMilli = Integer
            .parseInt(System.getProperty("deadApplierCheckIntervalMilli", "15000"));

    @Autowired
    private MetaServerConfig config;

    @Autowired
    private ApplierStateController applierStateController;

    @Autowired
    private DcMetaCache dcMetaCache;

    @Autowired
    private MultiDcService multiDcService;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = MetaServerContextConfig.CLIENT_POOL)
    private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;

    @Resource(name = AbstractSpringConfigContext.CLUSTER_SHARD_ADJUST_EXECUTOR)
    private KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long>> clusterShardExecutor;

    private ExecutorService executors;

    private ScheduledFuture<?> deadCheckFuture;

    private ScheduledFuture<?> applierInfoCheckFuture;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        executors = DefaultExecutorFactory
                .createAllowCoreTimeout("DefaultApplierManager", Math.min(4, OsUtils.defaultMaxCoreThreadCount()))
                .createExecutorService();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        deadCheckFuture = scheduled.scheduleWithFixedDelay(new DeadApplierChecker(), deadApplierCheckIntervalMilli,
                deadApplierCheckIntervalMilli, TimeUnit.MILLISECONDS);

        applierInfoCheckFuture = scheduled.scheduleWithFixedDelay(new ApplierStateAlignChecker(), config.getApplierInfoCheckInterval(),
                config.getApplierInfoCheckInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        deadCheckFuture.cancel(true);
        applierInfoCheckFuture.cancel(true);
        executors.shutdownNow();
    }

    @Override
    protected void handleClusterAdd(ClusterMeta clusterMeta) {
        for (SourceMeta sourceMeta : clusterMeta.getSources()) {
            for (ShardMeta shardMeta : sourceMeta.getShards().values()) {
                for (ApplierMeta applierMeta : shardMeta.getAppliers()) {
                    addApplier(clusterMeta.getDbId(), shardMeta.getDbId(), applierMeta);
                }
            }
        }
    }

    @Override
    protected void handleClusterModified(ClusterMetaComparator comparator) {
        ClusterMeta cluster = comparator.getCurrent();
        comparator.accept(new ClusterComparatorVisitor(cluster.getDbId()));
    }

    @Override
    protected void handleClusterDeleted(ClusterMeta clusterMeta) {
        for (SourceMeta sourceMeta : clusterMeta.getSources()) {
            for (ShardMeta shardMeta : sourceMeta.getShards().values()) {
                for (ApplierMeta applierMeta : shardMeta.getAppliers()) {
                    removeApplier(clusterMeta.getDbId(), shardMeta.getDbId(), applierMeta);
                }
            }
        }
    }

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
//        return Collections.singleton(ClusterType.HETERO);
        // TODO: 2022/10/10 remove hetero
        return Collections.singleton(ClusterType.ONE_WAY);
    }

    protected List<ApplierMeta> getDeadAppliers(List<ApplierMeta> allAppliers, List<ApplierMeta> aliveAppliers) {
        List<ApplierMeta> result = new LinkedList<>();
        for (ApplierMeta allOne : allAppliers) {
            boolean alive = false;
            for (ApplierMeta aliveOne : aliveAppliers) {
                if (ObjectUtils.equals(aliveOne.getIp(), allOne.getIp())
                        && ObjectUtils.equals(aliveOne.getPort(), allOne.getPort())) {
                    alive = true;
                    break;
                }
            }
            if (!alive) {
                result.add(allOne);
            }
        }
        return result;
    }

    private void addApplier(Long clusterDbId, Long shardDbId, ApplierMeta applierMeta) {
        try {
            applierStateController.addApplier(new ApplierTransMeta(applierMeta.getTargetClusterName(), clusterDbId, shardDbId, applierMeta));
        } catch (Exception e) {
            logger.error(String.format("[addApplier]cluster_%s:shard_%s,%s", clusterDbId, shardDbId, applierMeta), e);
        }
    }

    private void removeApplier(Long clusterDbId, Long shardDbId, ApplierMeta applierMeta) {
        try {
            applierStateController.removeApplier(new ApplierTransMeta(clusterDbId, shardDbId, applierMeta));
        } catch (Exception e) {
            logger.error(String.format("[removeApplier]cluster_%s:shard_%s,%s", clusterDbId, shardDbId, applierMeta), e);
        }
    }

    public class DeadApplierChecker extends AbstractApplierStateChecker {
        protected void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {
            Long clusterDbId = clusterMeta.getDbId();
            Long shardDbId = shardMeta.getDbId();
            List<ApplierMeta> allAppliers = shardMeta.getAppliers();
            List<ApplierMeta> aliveAppliers = currentMetaManager.getSurviveAppliers(clusterDbId, shardDbId);
            List<ApplierMeta> deadAppliers = getDeadAppliers(allAppliers, aliveAppliers);

            if (deadAppliers.size() > 0) {
                logger.info("[doCheck][dead appliers]{}", deadAppliers);
            }
            for (ApplierMeta deadApplier : deadAppliers) {
                try {
                    applierStateController.addApplier(new ApplierTransMeta(deadApplier.getTargetClusterName(), clusterDbId, shardDbId, deadApplier));
                } catch (ResourceAccessException e) {
                    logger.error(String.format("cluster_%d,shard_%d, applier:%s, error:%s", clusterDbId, shardDbId,
                            deadApplier, e.getMessage()));
                } catch (Throwable th) {
                    logger.error("[doCheck]", th);
                }
            }
        }
    }

    protected class ClusterComparatorVisitor implements MetaComparatorVisitor<ShardMeta> {

        private Long clusterDbId;

        public ClusterComparatorVisitor(Long clusterDbId) {
            this.clusterDbId = clusterDbId;
        }

        @Override
        public void visitAdded(ShardMeta added) {
            logger.info("[visitAdded][add shard]{}", added);
            for (ApplierMeta applierMeta : added.getAppliers()) {
                addApplier(clusterDbId, added.getDbId(), applierMeta);
            }
        }

        @Override
        public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {
            ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator;
            ShardMeta shard = shardMetaComparator.getCurrent();
            shardMetaComparator.accept(new ShardComparatorVisitor(clusterDbId, shard.getDbId()));
        }

        @Override
        public void visitRemoved(ShardMeta removed) {
            logger.info("[visitRemoved][remove shard]{}", removed);
            for (ApplierMeta applierMeta : removed.getAppliers()) {
                removeApplier(clusterDbId, removed.getDbId(), applierMeta);
            }
        }
    }

    protected class ShardComparatorVisitor implements MetaComparatorVisitor<InstanceNode> {

        private Long clusterDbId;
        private Long shardDbId;

        protected ShardComparatorVisitor(Long clusterDbId, Long shardDbId) {
            this.clusterDbId = clusterDbId;
            this.shardDbId = shardDbId;
        }

        @Override
        public void visitAdded(InstanceNode added) {
            if (added instanceof ApplierMeta) {
                addApplier(clusterDbId, shardDbId, (ApplierMeta) added);
            } else {
                logger.debug("[visitAdded][do nothing]{}", added);
            }
        }

        @Override
        public void visitModified(MetaComparator comparator) {
            // nothing to do
        }

        @Override
        public void visitRemoved(InstanceNode removed) {
            if (removed instanceof ApplierMeta) {
                removeApplier(clusterDbId, shardDbId, (ApplierMeta) removed);
            } else {
                logger.debug("[visitRemoved][do nothing]{}", removed);
            }
        }
    }

    interface ApplierStateChecker extends Runnable {}

    private abstract class AbstractApplierStateChecker implements ApplierStateChecker {
        @Override
        public void run() {

            try {
                doCheck();
            } catch (Throwable th) {
                logger.error("[run]", th);
            }

        }

        protected void doCheck() {
            for (Long clusterDbId : currentMetaManager.allClusters()) {
                ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterDbId);
                if (!supportCluster(clusterMeta)) continue;
                for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
                    doCheckShard(clusterMeta, shardMeta);
                }
            }
        }

        protected abstract void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta);
    }

    private final String STATE = "state";
    private final String MASTER_HOST = "master_host";
    private final String MASTER_PORT = "master_port";

    public class ApplierStateAlignChecker extends AbstractApplierStateChecker {

        @Override
        protected void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {
            Long clusterDbId = clusterMeta.getDbId();
            Long shardDbId = shardMeta.getDbId();
            List<ApplierMeta> applierMetas = currentMetaManager.getSurviveAppliers(clusterDbId, shardDbId);
            if (applierMetas.isEmpty()) return;

            SequenceCommandChain sequenceCommandChain =
                    new SequenceCommandChain(String.format("%s-cluster_%d-shard_%d", this.getClass().getSimpleName(), clusterDbId, shardDbId));
            for (ApplierMeta applierMeta : applierMetas) {
                InfoCommand infoCommand = new InfoCommand(
                        clientPool.getKeyPool(new DefaultEndPoint(applierMeta.getIp(), applierMeta.getPort())),
                        InfoCommand.INFO_TYPE.REPLICATION,
                        scheduled
                );
                infoCommand.logRequest(false);
                infoCommand.logResponse(false);
                CausalChain causalChain = new CausalChain();
                causalChain.add(infoCommand);
                causalChain.add(new ApplierInfoCheckCommand(applierMeta, clusterDbId, shardDbId));
                sequenceCommandChain.add(causalChain);
            }
            sequenceCommandChain.execute(executors).addListener(new CommandFutureListener<Object>() {
                @Override
                public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                    if (!commandFuture.isSuccess()) {
                        doCorrect(clusterDbId, shardDbId, applierMetas);
                    }
                }
            });
        }

        protected void doCorrect(Long clusterDbId, Long shardDbId, List<ApplierMeta> survivedAppliers) {
            //TODO ayq should stop always false
            //TODO ayq change applier master when jvm restart

            ApplierStateChangeJob job = createApplierStateChangeJob(clusterDbId, shardDbId, survivedAppliers,
                    currentMetaManager.getApplierMaster(clusterDbId, shardDbId));
            job.future().addListener(new CommandFutureListener<Void>() {
                @Override
                public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
                    logger.info("[{}][ApplierInfoCorrect]cluster_{}, shard_{}, result: {}",
                            getClass(), clusterDbId, shardDbId, commandFuture.isSuccess());
                }
            });
            clusterShardExecutor.execute(Pair.from(clusterDbId, shardDbId), job);
        }
    }

    public class ActiveApplierInfoChecker extends AbstractApplierInfoChecker {

        private Pair<String, Integer> master;

        public ActiveApplierInfoChecker(InfoResultExtractor extractor, Long clusterDbId, Long shardDbId) {
            super(extractor, clusterDbId, shardDbId);
            master = currentMetaManager.getApplierMaster(clusterDbId, shardDbId);
        }

        @Override
        protected boolean shouldStop() {
            //TODO ayq false or others?
            return false;
        }

        @Override
        protected boolean isApplierStateOk() {
            return ApplierState.ACTIVE.name().equals(extractor.extract(STATE));
        }

        @Override
        protected boolean isMasterMatched() {
            return null != master && getMasterHost().equals(extractor.extract(MASTER_HOST))
                    && getMasterPort() == Integer.parseInt(extractor.extract(MASTER_PORT));
        }

        private String getMasterHost() {
            return master.getKey();
        }

        private int getMasterPort() {
            return master.getValue();
        }
    }

    private ApplierStateChangeJob createApplierStateChangeJob(Long clusterDbId, Long shardDbId, List<ApplierMeta> appliers,
                                                            Pair<String, Integer> master) {

        //TODO ayq route
        RouteMeta routeMeta = currentMetaManager.getClusterRouteByDcId(currentMetaManager.getClusterMeta(clusterDbId).getActiveDc(), clusterDbId);

        String srcSids = currentMetaManager.getSrcSids(clusterDbId, shardDbId);
        GtidSet gtidSet = currentMetaManager.getGtidSet(clusterDbId, srcSids);

        return new ApplierStateChangeJob(appliers, master, srcSids, gtidSet, routeMeta, clientPool, scheduled, executors);
    }

    public class BackupApplierInfoChecker extends AbstractApplierInfoChecker {

        public BackupApplierInfoChecker(InfoResultExtractor extractor, Long clusterDbId, Long shardDbId) {
            super(extractor, clusterDbId, shardDbId);
        }

        @Override
        protected boolean isApplierStateOk() {
            return ApplierState.BACKUP.name().equals(extractor.extract(STATE));
        }

        @Override
        protected boolean isMasterMatched() {
            return (extractor.extract(MASTER_HOST) == null || extractor.extract(MASTER_HOST).equals("0")) &&
                    (extractor.extract(MASTER_PORT) == null || extractor.extract(MASTER_PORT).equals("0"));
        }
    }

    protected abstract class AbstractApplierInfoChecker {

        protected InfoResultExtractor extractor;
        protected Long clusterDbId, shardDbId;

        public AbstractApplierInfoChecker(InfoResultExtractor extractor, Long clusterDbId, Long shardDbId) {
            this.extractor = extractor;
            this.clusterDbId = clusterDbId;
            this.shardDbId = shardDbId;
        }

        protected boolean shouldStop() {
            return false;
        }

        protected boolean isValid() {
            return isApplierStateOk() && isMasterMatched();
        }

        protected abstract boolean isMasterMatched();

        protected abstract boolean isApplierStateOk();
    }

    protected class ApplierInfoCheckCommand extends CausalCommand<String, Boolean> {

        private ApplierMeta applierMeta;

        private Long clusterDbId, shardDbId;

        private AbstractApplierInfoChecker infoChecker;

        public ApplierInfoCheckCommand(ApplierMeta applierMeta, Long clusterDbId, Long shardDbId) {
            this.applierMeta = applierMeta;
            this.clusterDbId = clusterDbId;
            this.shardDbId = shardDbId;
        }

        @Override
        protected void onSuccess(String info) {
            InfoResultExtractor extractor = new InfoResultExtractor(info);
            infoChecker = createApplierInfoChecker(applierMeta, clusterDbId, shardDbId, extractor);
            if(infoChecker.isValid() || infoChecker.shouldStop()) {
                future().setSuccess();
            } else {
                future().setFailure(new ApplierStateInCorrectException("Applier Role not correct"));
            }

        }

        @Override
        protected void onFailure(Throwable throwable) {
            if (ExceptionUtils.getRootCause(throwable) instanceof CommandTimeoutException) {
                logger.debug("[onFailure][cluster_{}][shard_{}] ignore failure for command timeout", clusterDbId, shardDbId);
                future().setSuccess();
            } else {
                future().setFailure(throwable);
            }
        }
    }

    private AbstractApplierInfoChecker createApplierInfoChecker(ApplierMeta applier, Long clusterDbId, Long shardDbId,
                                                                InfoResultExtractor extractor) {
        if (applier.isActive()) {
            return new ActiveApplierInfoChecker(extractor, clusterDbId, shardDbId);
        } else {
            return new BackupApplierInfoChecker(extractor, clusterDbId, shardDbId);
        }
    }

    @VisibleForTesting
    public void setApplierStateController(ApplierStateController applierStateController) {
        this.applierStateController = applierStateController;
    }
}
