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

    public static int DEFAULT_REDIS_GTID_COLLECT_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("REDIS_GTID_COLLECT_INTERVAL_SECONDS", "2"));

    private MultiDcService multiDcService;

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private int collectIntervalSeconds;

    public DefaultRedisGtidCollector(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                     CurrentMetaManager currentMetaManager, MultiDcService multiDcService,
                                     ScheduledExecutorService scheduled, XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, multiDcService, scheduled, keyedObjectPool,
                DEFAULT_REDIS_GTID_COLLECT_INTERVAL_SECONDS);
    }

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
        List<RedisMeta> redises = currentMetaManager.getRedises(clusterDbId, shardDbId);
        logger.info("[work][collect gtid]cluster_{}, shard_{}", clusterDbId, shardDbId);

        for (RedisMeta redisMeta : redises) {
            try {
                logger.info("[work][collect gtid][redis]ip={}, port={}", redisMeta.getIp(), redisMeta.getPort());
                collectGtidAndSids(redisMeta);
            } catch (Throwable th) {
                logger.warn("[work][collect gtid][redis] failed, ip={}, port={}", redisMeta.getIp(), redisMeta.getPort(), th);
            }
        }

        if (!dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)) {
            String currentDc = dcMetaCache.getCurrentDc();
            String srcDcName = dcMetaCache.getSrcDc(currentDc, clusterDbId, shardDbId);
            String srcSids = multiDcService.getSids(currentDc, srcDcName, clusterDbId, shardDbId);

            String currentSrcSids = currentMetaManager.getSrcSids(clusterDbId, shardDbId);
            if (sidsChanged(srcSids, currentSrcSids)) {
                currentMetaManager.setSrcSidsAndNotify(clusterDbId, shardDbId, srcSids);
            }
        }
    }

    @VisibleForTesting
    public boolean sidsChanged(String sids, String currentSids) {
        if (StringUtils.isEmpty(sids)) {
            return false;
        }
        if (StringUtils.isEmpty(currentSids)) {
            return true;
        }

        Set<String> sidSet = new HashSet<>(Arrays.asList(sids.split(",")));
        Set<String> currentSidSet = new HashSet<>(Arrays.asList(currentSids.split(",")));

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
            logger.info("[info gtid command Complete], cluster_{}, shard_{}", clusterDbId, shardDbId);
            if (commandFuture.isSuccess()) {
                GtidSet gtidSet = commandFuture.get();
                if (gtidSet == null) {
                    logger.warn("[info gtid command return null], cluster_{}, shard_{}", clusterDbId, shardDbId);
                    return;
                }
                String sids = null;
                if (!gtidSet.getUUIDs().isEmpty()) {
                    for(String sid: gtidSet.getUUIDs()) {
                        sids = sids == null? sid: sids + "," + sid;
                    }
                }
                redisMeta.setGtid(gtidSet.toString());
                redisMeta.setSid(sids);
            } else {
                logger.error("[info gtid command failed], cluster_{}, shard_{}", clusterDbId, shardDbId, commandFuture.cause());
            }
        });
    }
}
