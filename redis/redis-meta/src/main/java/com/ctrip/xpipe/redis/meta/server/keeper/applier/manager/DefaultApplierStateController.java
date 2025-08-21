package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerService;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerServiceFactory;
import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierStateController;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.CLIENT_POOL;

/**
 * @author ayq
 * <p>
 * 2022/4/1 23:25
 */
public class DefaultApplierStateController extends AbstractLifecycle implements ApplierStateController, TopElement {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private int addApplierSuccessTimeoutMilli =    180000;
    private int removeApplierSuccessTimeoutMilli = 60000;

    @Autowired
    private DcMetaCache dcMetaCache;

    @Autowired
    private ApplierContainerServiceFactory applierContainerServiceFactory;

    @Resource( name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool clientKeyedObjectPool;

    private ExecutorService executors;

    private KeyedOneThreadTaskExecutor<Pair<Long, Long>> shardExecutor;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        executors = DefaultExecutorFactory.createAllowCoreTimeout("applierStateController", OsUtils.defaultMaxCoreThreadCount()).createExecutorService();
        shardExecutor = new KeyedOneThreadTaskExecutor<>(executors);
    }

    @Override
    protected void doDispose() throws Exception {

        shardExecutor.destroy();
        executors.shutdown();
        super.doDispose();
    }

    @Override
    public void addApplier(ApplierTransMeta applierTransMeta) {
        logger.info("[addApplier]{}", applierTransMeta);

        ApplierContainerService applierContainerService = getApplierContainerService(applierTransMeta);
        shardExecutor.execute(new Pair<>(applierTransMeta.getClusterDbId(), applierTransMeta.getShardDbId()),
                createAddApplierCommand(applierContainerService, applierTransMeta, scheduled, addApplierSuccessTimeoutMilli));
    }

    @Override
    public void removeApplier(ApplierTransMeta applierTransMeta) {
        logger.info("[removeApplier]{}", applierTransMeta);

        ApplierContainerService applierContainerService = getApplierContainerService(applierTransMeta);
        shardExecutor.execute(new Pair<>(applierTransMeta.getClusterDbId(), applierTransMeta.getShardDbId()),
                createDeleteApplierCommand(applierContainerService, applierTransMeta, scheduled, removeApplierSuccessTimeoutMilli));
    }

    protected ApplierContainerService getApplierContainerService(ApplierTransMeta applierTransMeta) {

        ApplierContainerMeta applierContainerMeta = dcMetaCache.getApplierContainer(applierTransMeta.getApplierMeta());
        ApplierContainerService applierContainerService = applierContainerServiceFactory.getOrCreateApplierContainerService(applierContainerMeta);

        return applierContainerService;
    }

    protected Command<?> createAddApplierCommand(ApplierContainerService applierContainerService,
                                                 ApplierTransMeta applierTransMeta, ScheduledExecutorService scheduled, int addApplierSuccessTimeoutMilli) {
        return new AddApplierCommand(applierContainerService, applierTransMeta, clientKeyedObjectPool, scheduled, addApplierSuccessTimeoutMilli);
    }

    protected Command<?> createDeleteApplierCommand(ApplierContainerService applierContainerService,
                                                 ApplierTransMeta applierTransMeta, ScheduledExecutorService scheduled, int removeApplierSuccessTimeoutMilli) {
        return new DeleteApplierCommand(applierContainerService, applierTransMeta, scheduled, removeApplierSuccessTimeoutMilli);
    }

    @VisibleForTesting
    public void setExecutors(ExecutorService executors) {
        this.executors = executors;
    }
}
