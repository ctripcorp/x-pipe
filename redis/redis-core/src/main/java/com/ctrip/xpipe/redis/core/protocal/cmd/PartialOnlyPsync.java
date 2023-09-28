package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.tuple.Pair;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2023/8/3
 */
public class PartialOnlyPsync extends AbstractReplicationStorePsync {

    private ReplicationStoreManager replicationStoreManager;

    private Endpoint masterEndPoint;

    public PartialOnlyPsync(SimpleObjectPool<NettyClient> clientPool,
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

    @Override
    protected Pair<String, Long> getRequestMasterInfo() {

        String replIdRequest;
        long offset;
        if (currentReplicationStore == null || currentReplicationStore.isFresh()) {
            replIdRequest = "?";
            offset = KEEPER_PARTIAL_SYNC_OFFSET;
        } else {
            replIdRequest = currentReplicationStore.getMetaStore().getReplId();
            offset = currentReplicationStore.getEndOffset() + 1;
        }
        return new Pair<>(replIdRequest, offset);
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

    @Override
    protected void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) throws IOException {
        throw new IllegalStateException("impossible to be here");
    }

    @Override
    public String toString() {
        return "keeperpsync->"  + masterEndPoint;
    }

}
