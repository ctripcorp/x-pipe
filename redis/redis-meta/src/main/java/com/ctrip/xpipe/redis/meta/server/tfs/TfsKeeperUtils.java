package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * TFS keeper classification helpers.
 */
public final class TfsKeeperUtils {

    private static final Logger logger = LoggerFactory.getLogger(TfsKeeperUtils.class);

    private TfsKeeperUtils() {
    }

    public static boolean isTfsKeeper(KeeperMeta keeperMeta, DcMetaCache dcMetaCache) {
        if (keeperMeta == null || dcMetaCache == null) {
            return false;
        }
        try {
            KeeperContainerMeta keeperContainer = dcMetaCache.getKeeperContainer(keeperMeta);
            if (keeperContainer == null) {
                logger.warn("[isTfsKeeper][keeper container missing, treat as non-TFS]{}", keeperMeta);
                return false;
            }
            return KeeperDiskTypeUtils.isTfs(keeperContainer.getDiskType());
        } catch (IllegalArgumentException e) {
            logger.warn("[isTfsKeeper][keeper container not found, treat as non-TFS]{}", keeperMeta);
            return false;
        }
    }

    public static boolean shardHasTfsKeeper(List<KeeperMeta> keepers, DcMetaCache dcMetaCache) {
        if (keepers == null) {
            return false;
        }
        for (KeeperMeta keeperMeta : keepers) {
            if (isTfsKeeper(keeperMeta, dcMetaCache)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Survive first (same {@code ip:port} keeps the survive object); then append meta-only keepers.
     * {@code meta == null} → survive only.
     */
    public static List<KeeperMeta> mergeByIpPort(List<KeeperMeta> survive, List<KeeperMeta> meta) {
        List<KeeperMeta> union = new LinkedList<>();
        if (survive != null) {
            union.addAll(survive);
        }
        if (meta == null) {
            return union;
        }
        for (KeeperMeta metaKeeper : meta) {
            if (!containsByIpPort(union, metaKeeper)) {
                union.add(metaKeeper);
            }
        }
        return union;
    }

    /**
     * Role / Step1 PREPARE list: survive ∪ {@code getShardKeepers}.
     * Null or thrown {@code getShardKeepers} falls back to survive (D38); does not block switch.
     */
    public static List<KeeperMeta> mergeSurviveAndShardKeepers(List<KeeperMeta> survive, DcMetaCache dcMetaCache,
                                                               Long clusterDbId, Long shardDbId) {
        if (dcMetaCache == null) {
            return mergeByIpPort(survive, null);
        }
        try {
            return mergeByIpPort(survive, dcMetaCache.getShardKeepers(clusterDbId, shardDbId));
        } catch (RuntimeException e) {
            logger.error("[mergeSurviveAndShardKeepers][getShardKeepers failed, fallback survive]cluster_{},shard_{}",
                    clusterDbId, shardDbId, e);
            return mergeByIpPort(survive, null);
        }
    }

    private static boolean containsByIpPort(List<KeeperMeta> keepers, KeeperMeta target) {
        for (KeeperMeta keeperMeta : keepers) {
            if (MetaUtils.same(keeperMeta, target)) {
                return true;
            }
        }
        return false;
    }
}
