package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoGtidCommand;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/13 22:54
 */
public class RedisGtidCollectJob extends AbstractCommand<Void> {

    private Long clusterDbId;

    private Long shardDbId;

    private DcMetaCache dcMetaCache;

    private ScheduledExecutorService scheduled;

    private XpipeNettyClientKeyedObjectPool pool;

    public RedisGtidCollectJob(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                               ScheduledExecutorService schedule, XpipeNettyClientKeyedObjectPool pool) {
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
        this.dcMetaCache = dcMetaCache;
        this.scheduled = schedule;
        this.pool = pool;
    }

    @Override
    protected void doExecute() throws Exception {
        getLogger().debug("[doExecute]cluster_{}, shard_{}", clusterDbId, shardDbId);

        if (!ClusterType.HETERO.equals(dcMetaCache.getClusterType(clusterDbId))) {
            getLogger().info("[doExecute][collect skip] cluster_{}, shard_{} is not hetero type", clusterDbId, shardDbId);
            future().setSuccess();
            return;
        }

        List<RedisMeta> redises = dcMetaCache.getShardRedises(clusterDbId, shardDbId);
        if (null == redises || redises.isEmpty()) {
            getLogger().info("[doExecute][collect skip] cluster_{}, shard_{} redisList empty", clusterDbId, shardDbId);
            future().setSuccess();
            return;
        }

        getLogger().info("[doExecute][collect gtid]cluster_{}, shard_{}", clusterDbId, shardDbId);

        for (RedisMeta redisMeta : redises) {

            //TODO ayq different pool; future().setSuccess()
            SimpleObjectPool<NettyClient> simpleObjectPool = new XpipeObjectPoolFromKeyed<Endpoint, NettyClient>(
                    pool, new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort()));

            Command<GtidSet> command = new InfoGtidCommand(simpleObjectPool, scheduled);
            command.execute().addListener(commandFuture -> {
                getLogger().info("[info gtid command Complete], cluster_{}, shard_{}", clusterDbId, shardDbId);
                if (commandFuture.isSuccess()) {
                    GtidSet gtidSet = commandFuture.get();
                    if (gtidSet == null) {
                        getLogger().warn("[info gtid command return null], cluster_{}, shard_{}", clusterDbId, shardDbId);
                        return;
                    }
                    String sids = null;
                    if (!gtidSet.getUUIDs().isEmpty()) {
                        for(String sid: gtidSet.getUUIDs()) {
                            sids = sids == null? sid: sids + "," + sid;
                        }
                    }

                    dcMetaCache.setRedisGtidAndSids(clusterDbId, shardDbId, redisMeta, gtidSet.toString(), sids);
                } else {
                    getLogger().error("[info gtid command failed], cluster_{}, shard_{}", clusterDbId, shardDbId, commandFuture.cause());
                }
            });
        }
    }

    @Override
    protected void doReset() {

    }

    @Override
    public String getName() {
        return "redis gtid collect job";
    }
}
