package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.tuple.Pair;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

public class PartialOnlyGapAllowedSync extends AbstractReplicationStoreGapAllowedSync {

    private ReplicationStoreManager replicationStoreManager;

    private Endpoint masterEndPoint;

    public PartialOnlyGapAllowedSync(SimpleObjectPool<NettyClient> clientPool,
                                     Endpoint masterEndPoint, ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled) {
        super(clientPool, true, scheduled);
        this.masterEndPoint = masterEndPoint;
        this.replicationStoreManager = replicationStoreManager;
        this.currentReplicationStore = getCurrentReplicationStore();
    }

    @Override
    protected final ReplicationStore getCurrentReplicationStore() {

        try {
            return replicationStoreManager.createIfNotExist();
        } catch (IOException e) {
            getLogger().error("[getCurrentReplicationStore][{}]{}", this, replicationStoreManager, e);
            throw new XpipeRuntimeException("[getCurrentReplicationStore]getReplicationStore failed." + replicationStoreManager, e);
        }
    }

    public SyncRequest getSyncRequest() {
        if(currentReplicationStore == null || currentReplicationStore.isFresh()) {
            PsyncRequest partial = new PsyncRequest();
            partial.setReplId("?");
            partial.setReplOff(KEEPER_PARTIAL_SYNC_OFFSET);
            return partial;
        }

        if (currentReplicationStore.getMetaStore().getCurrentReplStage().getProto() != ReplStage.ReplProto.XSYNC) {
            PsyncRequest psync = new PsyncRequest();
            psync.setReplId(currentReplicationStore.getMetaStore().getReplId());
            psync.setReplOff(currentReplicationStore.getEndOffset() + 1);
            return psync;
        } else {
            XsyncRequest xsync = new XsyncRequest();
            Pair<GtidSet, GtidSet> gtidSets = currentReplicationStore.getGtidSet();
            GtidSet gtidSet = gtidSets.getKey().union(gtidSets.getValue());
            xsync.setUuidIntrested(UUID_INSTRESTED_DEFAULT);
            xsync.setGtidSet(gtidSet);
            return xsync;
        }
    }

    @Override
    protected void doOnFullSync() throws IOException {
        if(currentReplicationStore != null && !currentReplicationStore.isFresh()) {
            try {
                getLogger().info("[refullsync][reset store]{}, {}", this, currentReplicationStore);
                replicationStoreManager.create();
            } catch (Throwable th) {
                getLogger().error("[refullsync]{}", currentReplicationStore, th);
            }
            notifyReFullSync();
        }
        getLogger().info("[doOnFullSync][terminate and try later]{}", this);
        throw new XpipeRuntimeException("only partial sync allow");
    }
    protected void doOnXFullSync() throws IOException {
        doOnFullSync();
    }

    @Override
    protected void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) throws IOException {
        throw new IllegalStateException("impossible to be here");
    }

    @Override
    public String toString() {
        return "keeperpsync->"  + masterEndPoint;
    }

}
