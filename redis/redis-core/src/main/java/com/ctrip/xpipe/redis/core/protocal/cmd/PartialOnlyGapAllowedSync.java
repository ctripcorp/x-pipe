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
import java.util.function.IntSupplier;

public class PartialOnlyGapAllowedSync extends AbstractReplicationStoreGapAllowedSync {

    private ReplicationStoreManager replicationStoreManager;

    private Endpoint masterEndPoint;

    public PartialOnlyGapAllowedSync(SimpleObjectPool<NettyClient> clientPool,
                                     Endpoint masterEndPoint, ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled, IntSupplier maxGap) {
        super(clientPool, true, scheduled, maxGap);
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
        } else {
            return getReplicationStoreSyncRequest();
        }
    }

    private void doOnFullSync0(String logPrefix) throws IOException {
        if (currentReplicationStore != null && !currentReplicationStore.isFresh()) {
            try {
                getLogger().info("[{}][refullsync][reset store]{}, {}", logPrefix, this, currentReplicationStore);
                replicationStoreManager.create();
            } catch (Throwable th) {
                getLogger().error("[{}][refullsync]{}", logPrefix, currentReplicationStore, th);
            }
            notifyReFullSync();
        }
        getLogger().info("[{}][terminate and try later]{}", logPrefix, this);
        throw new XpipeRuntimeException("only partial sync allow");
    }
    @Override
    protected void doOnFullSync() throws IOException {
        doOnFullSync0("doOnFullSync");
    }
    protected void doOnXFullSync() throws IOException {
        doOnFullSync0("doOnXFullSync");
    }

    @Override
    protected void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) throws IOException {
        throw new IllegalStateException("impossible to be here");
    }

    @Override
    public String toString() {
        return "gakeeperpsync->"  + masterEndPoint;
    }

}
