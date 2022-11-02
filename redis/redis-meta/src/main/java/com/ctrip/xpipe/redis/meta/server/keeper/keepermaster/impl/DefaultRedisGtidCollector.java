package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoGtidCommand;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.RedisGtidCollector;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/8/22 15:38
 */
public class DefaultRedisGtidCollector extends AbstractClusterShardPeriodicTask implements RedisGtidCollector {

    public static int REDIS_INFO_GTID_INTERVAL_SECONDS_DR_MASTER_GROUP = Integer
            .parseInt(System.getProperty("REDIS_INFO_GTID_INTERVAL_SECONDS_DR_MASTER_GROUP", "30"));

    public static int REDIS_INFO_GTID_INTERVAL_SECONDS_MASTER_GROUP  = Integer
            .parseInt(System.getProperty("REDIS_INFO_GTID_INTERVAL_SECONDS_DR_MASTER_GROUP", "2"));

    private MultiDcService multiDcService;

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private int collectIntervalSeconds;

    public DefaultRedisGtidCollector(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                     CurrentMetaManager currentMetaManager, MultiDcService multiDcService,
                                     ScheduledExecutorService scheduled, XpipeNettyClientKeyedObjectPool keyedObjectPool, int collectIntervalSeconds) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
        this.multiDcService = multiDcService;
        this.keyedObjectPool = keyedObjectPool;
        this.collectIntervalSeconds = collectIntervalSeconds;
    }

    @Override
    protected void work() {

        collectCurrentDcGtidAndSids();
        collectSids();
    }

    private void collectCurrentDcGtidAndSids() {

        /* redis shard */
        List<RedisMeta> redises = currentMetaManager.getRedises(clusterDbId, shardDbId);
        logger.debug("[work][collect gtid]cluster_{}, shard_{}", clusterDbId, shardDbId);

        for (RedisMeta redisMeta : redises) {
            try {
                logger.info("[work][collect gtid][cluster_{}][shard_{}][redis]ip={}, port={}", clusterDbId, shardDbId, redisMeta.getIp(), redisMeta.getPort());
                collectGtidAndSids(redisMeta);
            } catch (Throwable th) {
                logger.warn("[work][collect gtid][cluster_{}][shard_{}][redis] failed, ip={}, port={}", clusterDbId, shardDbId, redisMeta.getIp(), redisMeta.getPort(), th);
            }
        }
    }

    private void collectSids() {

        if (dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId) ||
            dcMetaCache.getShardAppliers(clusterDbId, shardDbId) == null ||
            dcMetaCache.getShardAppliers(clusterDbId, shardDbId).isEmpty()) {
            return;
        }

        /* applier shard */
        String currentDc = dcMetaCache.getCurrentDc();
        String srcDcName = dcMetaCache.getSrcDc(currentDc, clusterDbId, shardDbId);
        String srcSids = multiDcService.getSids(currentDc, srcDcName, clusterDbId, shardDbId);

        String currentSrcSids = currentMetaManager.getSrcSids(clusterDbId, shardDbId);
        if (sidsChanged(srcSids, currentSrcSids)) {
            currentMetaManager.setSrcSidsAndNotify(clusterDbId, shardDbId, srcSids);
        }
    }

    @VisibleForTesting
    public boolean sidsChanged(String sids, String cachedSids) {
        if (StringUtils.isEmpty(sids)) {
            return false;
        }
        if (StringUtils.isEmpty(cachedSids)) {
            return true;
        }

        Set<String> sidSet = new HashSet<>(Arrays.asList(sids.split(",")));
        Set<String> currentSidSet = new HashSet<>(Arrays.asList(cachedSids.split(",")));

        return sidSet.retainAll(currentSidSet) || currentSidSet.retainAll(sidSet);
    }

    @Override
    protected int getWorkIntervalSeconds() {
        return collectIntervalSeconds;
    }

    private void collectGtidAndSids(RedisMeta redisMeta) {

        SimpleObjectPool<NettyClient> simpleObjectPool = keyedObjectPool.getKeyPool(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort()));

        Command<GtidSet> command = new InfoGtidCommand(simpleObjectPool, scheduled);
        command.execute().addListener(commandFuture -> {
            if (commandFuture.isSuccess()) {
                GtidSet gtidSet = commandFuture.get();
                if (gtidSet == null) {
                    logger.warn("[info gtid command return null], cluster_{}, shard_{}", clusterDbId, shardDbId);
                    return;
                }
                logger.info("[info gtid command], cluster_{}, shard_{}, gtidSet={}", clusterDbId, shardDbId, gtidSet);
                String sids = null;
                if (!gtidSet.getUUIDs().isEmpty()) {
                    for(String sid: gtidSet.getUUIDs()) {
                        sids = sids == null? sid: sids + "," + sid;
                    }
                }
                if (!gtidSet.toString().isEmpty()) {
                    redisMeta.setGtid(gtidSet.toString());
                }
                if (sidsChanged(sids, redisMeta.getSid())) {
                    redisMeta.setSid(sids);
                }
            } else {
                logger.error("[info gtid command failed], cluster_{}, shard_{}", clusterDbId, shardDbId, commandFuture.cause());
            }
        });
    }
}
