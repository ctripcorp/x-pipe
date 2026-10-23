package com.ctrip.xpipe.redis.checker.healthcheck.keeperstate;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 解出跨机房 redis 延迟打点的 {@code keeperStoreType}：复制链路
 * {@code master → active dc slave → active dc keeper → backup dc keeper → backup dc slave}
 * 上，两个机房各贡献一位，{@code 1} 为 TFS、{@code 0} 为本地盘。第一位是主机房（active dc），
 * 第二位是该 redis 所在机房。
 *
 * <p>每一位的判定只看 meta 的静态磁盘类型，必要时才问实时状态：
 * <ul>
 *   <li>机房内无 TFS Keeper —— 链路只可能走本地盘，{@code 0}，不查缓存</li>
 *   <li>机房内全是 TFS Keeper —— 无论哪个 Keeper 在用，都是 TFS，{@code 1}，不查缓存</li>
 *   <li>TFS 与本地盘混部 —— 取决于当下哪个 Keeper 是 ACTIVE，此时才查
 *       {@link KeeperStateCache}；实时状态采不到则降级用 meta 的 {@code active} 标记</li>
 * </ul>
 *
 * <p>跨 region 的 backup dc 不部署 TFS Keeper，其第二位恒为本地盘，故不查该机房的缓存
 * —— 那批 Keeper 本就不在本 checker 上采集（{@code KeeperCheckSelector} 排除 cross-region）。
 */
@Component
public class KeeperStoreTypeResolver {

    private static final Logger logger = LoggerFactory.getLogger(KeeperStoreTypeResolver.class);

    private static final String LOCAL = "0";

    private static final String TFS = "1";

    private final MetaCache metaCache;

    private final KeeperStateCache keeperStateCache;

    @Autowired
    public KeeperStoreTypeResolver(MetaCache metaCache, KeeperStateCache keeperStateCache) {
        this.metaCache = metaCache;
        this.keeperStateCache = keeperStateCache;
    }

    /**
     * @return 两位字符串；非跨机房实例返回 null，调用方据此跳过该 tag。跨机房实例总能得出两位
     *         —— 实时状态采不到时降级用 meta，避免故障窗口里 tag 断档
     */
    public String resolve(RedisInstanceInfo info) {
        if (null == info || !hasKeeperChain(info)) {
            return null;
        }
        // 跨 region 的 backup dc 不部署 TFS Keeper，恒为本地盘；其 Keeper 也不在本 checker 的采集范围内
        String localDcBit = info.isCrossRegion() ? LOCAL : resolveBit(info.getDcId(), info);
        return resolveBit(info.getActiveDc(), info) + localDcBit;
    }

    /**
     * 只有单活（ONE_WAY / HETERO）集群的非 activeDc 实例才有 Keeper 复制链路。
     *
     * <p>{@code isInActiveDc()} 在 {@code activeDc} 为空时返回 false，而双向同步集群没有 activeDc，
     * 单靠它会把 BI_DIRECTION 实例当成跨机房实例 —— 那类集群压根不支持 Keeper
     * （{@link ClusterType#supportKeeper()} 为 false），查出来的只会是空列表凑出的 {@code 00}。
     */
    private boolean hasKeeperChain(RedisInstanceInfo info) {
        ClusterType clusterType = info.getClusterType();
        if (null == clusterType || !clusterType.supportKeeper() || StringUtil.isEmpty(info.getActiveDc())) {
            return false;
        }
        return !info.isInActiveDc();
    }

    private String resolveBit(String dcId, RedisInstanceInfo info) {
        List<KeeperMeta> keepers =
                metaCache.getKeeperOfDcClusterShardRef(dcId, info.getClusterId(), info.getShardId());
        if (null == keepers || keepers.isEmpty()) {
            return LOCAL;
        }

        List<KeeperMeta> tfsKeepers = new ArrayList<>();
        for (KeeperMeta keeper : keepers) {
            if (KeeperDiskTypeUtils.isTfs(metaCache.getKeeperDiskType(keeper))) {
                tfsKeepers.add(keeper);
            }
        }

        if (tfsKeepers.isEmpty()) {
            return LOCAL;
        }
        if (tfsKeepers.size() == keepers.size()) {
            return TFS;
        }

        // 混部：链路走在当下的 ACTIVE Keeper 上。一个 shard 在一个机房同时只应有一个 ACTIVE，
        // 任一 TFS Keeper 为 ACTIVE 即判 TFS。
        for (KeeperMeta tfsKeeper : tfsKeepers) {
            if (isActive(dcId, tfsKeeper)) {
                return TFS;
            }
        }
        return LOCAL;
    }

    /**
     * 实时状态优先，采不到时降级用 meta 的期望值。
     *
     * <p>{@link KeeperStateCache} 在样本过期后返回 null —— Keeper 失联或正在高可用切换时都会如此，
     * 而这个窗口有十几秒。若据此不打 tag，恰好是延迟突增那批点失去归因能力；而 meta 的 {@code active}
     * 标记虽然滞后于实际切换，但 meta-server 选出新 ACTIVE 后会更新它，故障不恢复时也能收敛到正确值。
     */
    private boolean isActive(String dcId, KeeperMeta tfsKeeper) {
        Boolean active = keeperStateCache.isActive(new HostPort(tfsKeeper.getIp(), tfsKeeper.getPort()));
        if (null != active) {
            return active;
        }
        logger.debug("[isActive][fallback to meta] dc={}, keeper={}:{}, metaActive={}",
                dcId, tfsKeeper.getIp(), tfsKeeper.getPort(), tfsKeeper.isActive());
        return tfsKeeper.isActive();
    }
}
