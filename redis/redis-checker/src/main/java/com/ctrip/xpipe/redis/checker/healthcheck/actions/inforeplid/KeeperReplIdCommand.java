package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.CrossRegionKeeperSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.unidal.tuple.Triple;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Second stage of an {@link InfoReplIdAction} check: consumes the redis' INFO, confirms the master
 * it reports is this dc's Keeper, then reads that Keeper's INFO and emits both repl-ids.
 *
 * <p>Everything but the last step runs in {@link #doExecute()}, i.e. on the executor the chain was
 * given. That matters because the parsing, the meta lookup (which deep-clones a shard) and
 * {@code findOrCreateSession} (a shared lock, possibly proxy registration) would otherwise land on
 * the netty event loop -- the thread that completed stage one.
 */
class KeeperReplIdCommand extends AbstractCommand<Triple<String, String, String>> {

    private static final String MASTER_REPLID = "master_replid";

    private static final String MASTER_REPLID2 = "master_replid2";

    private final RedisHealthCheckInstance instance;

    /** Stage one; already complete by the time {@link #doExecute()} runs. */
    private final CommandFuture<String> slaveInfoFuture;

    private final CrossRegionKeeperSessionManager crossRegionKeeperSessionManager;

    private final MetaCache metaCache;

    private final Executor executor;

    KeeperReplIdCommand(RedisHealthCheckInstance instance, CommandFuture<String> slaveInfoFuture,
                        CrossRegionKeeperSessionManager crossRegionKeeperSessionManager,
                        MetaCache metaCache, Executor executor) {
        this.instance = instance;
        this.slaveInfoFuture = slaveInfoFuture;
        this.crossRegionKeeperSessionManager = crossRegionKeeperSessionManager;
        this.metaCache = metaCache;
        this.executor = executor;
    }

    @Override
    protected void doExecute() {
        InfoResultExtractor slaveInfo = new InfoResultExtractor(slaveInfoFuture.getNow());
        String slaveReplId = slaveInfo.extract(MASTER_REPLID);
        String masterHost = slaveInfo.getKeyKeeperMasterHost();
        int masterPort = slaveInfo.getKeyKeeperMasterPort();
        if (slaveReplId == null || masterHost == null || masterPort <= 0) {
            getLogger().info("[doExecute][slave info incomplete] {}", instance.getCheckInfo().getHostPort());
            throw new IllegalStateException("slave info incomplete");
        }

        List<KeeperMeta> keepers = metaCache.getKeeperOfDcClusterShard(
                instance.getCheckInfo().getDcId(),
                instance.getCheckInfo().getClusterId(),
                instance.getCheckInfo().getShardId());
        boolean keeperInMeta = keepers.stream()
                .anyMatch(keeper -> masterHost.equals(keeper.getIp()) && masterPort == keeper.getPort());
        if (!keeperInMeta) {
            getLogger().info("[doExecute][keeper not in meta] {} master={}:{}",
                    instance.getCheckInfo().getHostPort(), masterHost, masterPort);
            throw new KeeperNotInMetaException(
                    String.format("keeper not in meta, master=%s:%d", masterHost, masterPort));
        }

        RedisSession upstream = crossRegionKeeperSessionManager
                .findOrCreateSession(new HostPort(masterHost, masterPort));
        new SessionInfoCommand(upstream).execute(executor)
                .addListener(keeperFuture -> complete(slaveReplId, keeperFuture));
    }

    private void complete(String slaveReplId, CommandFuture<?> keeperFuture) {
        if (!keeperFuture.isSuccess()) {
            future().setFailure(keeperFuture.cause());
            return;
        }

        InfoResultExtractor keeperInfo = new InfoResultExtractor((String) keeperFuture.getNow());
        String keeperReplId = keeperInfo.extract(MASTER_REPLID);
        String keeperReplId2 = keeperInfo.extract(MASTER_REPLID2);
        if (keeperReplId == null) {
            getLogger().info("[complete][keeper info incomplete] {}", instance.getCheckInfo().getHostPort());
            future().setFailure(new IllegalStateException("keeper info incomplete"));
            return;
        }

        boolean replIdMatch = slaveReplId.equals(keeperReplId)
                || (keeperReplId2 != null && slaveReplId.equals(keeperReplId2));
        if (!replIdMatch) {
            getLogger().info("[complete][replId mismatch] {} slaveReplId={}, keeperReplId={}, keeperReplId2={}",
                    instance.getCheckInfo().getHostPort(), slaveReplId, keeperReplId, keeperReplId2);
        }
        future().setSuccess(new Triple<>(slaveReplId, keeperReplId, keeperReplId2));
    }

    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }
}
