package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ApplierInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.lwm.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.xsync.ApplierCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.xsync.ApplierXsyncReplication;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultXsyncReplication;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:16
 */
public class DefaultApplierServer extends AbstractInstanceNode implements ApplierServer {

    /* component */

    @InstanceDependency
    public ApplierSequenceController sequence;

    @InstanceDependency
    public ApplierLwmManager lwmManager;

    @InstanceDependency
    public ApplierXsyncReplication replication;

    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public InstanceComponentWrapper<LeaderElector> leaderElectorWrapper;

    /* cardinal info */

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    //TODO: distinguish gtid_received, gtid_executed, gtid_in_request
    public AtomicReference<GtidSet> gtidSet;

    public final int listeningPort;

    public final ClusterId clusterId;

    public final ShardId shardId;

    public final ApplierMeta applierMeta;

    /* utility */

    @InstanceDependency
    public InstanceComponentWrapper<XpipeNettyClientKeyedObjectPool> pool;

    @InstanceDependency
    public RedisOpParser parser;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    private static final int DEFAULT_SCHEDULED_CORE_POOL_SIZE = 1;

    private static final int DEFAULT_KEYED_CLIENT_POOL_SIZE = 2;

    public DefaultApplierServer(String clusterName, ClusterId clusterId, ShardId shardId, ApplierMeta applierMeta,
                                LeaderElectorManager leaderElectorManager, RedisOpParser parser) throws Exception {
        this.sequence = new DefaultSequenceController();
        this.lwmManager = new DefaultLwmManager();
        this.replication = new DefaultXsyncReplication();
        this.dispatcher = new DefaultCommandDispatcher();

//        this.client = new CRedisAsyncClientFactory().getOrCreateClient(clusterName);
        this.client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient(clusterName);
        this.parser = parser;
        this.leaderElectorWrapper = new InstanceComponentWrapper<>(createLeaderElector(clusterId, shardId, applierMeta,
                leaderElectorManager));

        this.gtidSet = new AtomicReference<>();
        this.listeningPort = applierMeta.getPort();
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.applierMeta = applierMeta;

        String threadPoolName = String.format("applier:%s", StringUtil.makeSimpleName(clusterId.toString(), shardId.toString()));
        scheduled = Executors.newScheduledThreadPool(DEFAULT_SCHEDULED_CORE_POOL_SIZE,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "sch-" + threadPoolName));
        pool = new InstanceComponentWrapper<>(new XpipeNettyClientKeyedObjectPool(DEFAULT_KEYED_CLIENT_POOL_SIZE));
    }

    private LeaderElector createLeaderElector(ClusterId clusterId, ShardId shardId, ApplierMeta applierMeta,
                                              LeaderElectorManager leaderElectorManager) {

        String leaderElectionZKPath = MetaZkConfig.getApplierLeaderLatchPath(clusterId, shardId);
        String leaderElectionID = MetaZkConfig.getApplierLeaderElectionId(applierMeta);
        ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
        return leaderElectorManager.createLeaderElector(ctx);
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        scheduled.shutdown();
    }

    @Override
    public int getListeningPort() {
        return listeningPort;
    }

    @Override
    public ApplierInstanceMeta getApplierInstanceMeta() {
        return new ApplierInstanceMeta(clusterId, shardId, applierMeta);
    }

    @Override
    public void setState(Endpoint endpoint, GtidSet gtidSet) {
        replication.connect(endpoint, gtidSet);
    }

    @Override
    public SERVER_ROLE role() {
        return SERVER_ROLE.APPLIER;
    }
}
