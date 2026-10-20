package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 在 Console 下发给 Checker 的 Meta 出口处，把容器 Keeper 的 hostname 解析为实时网络 IP 写入 {@link KeeperMeta#setIp}。
 * <p>
 * 容器 Keeper 判定：{@code hostname} 非空（与 {@code MetaUtils.theSame}、{@code InverseHostPortMapBuilder} 口径一致）。
 * 物理机 Keeper（hostname 为空）原样保留，不查表。
 * <p>
 * copy-on-write：返回新的 XpipeMeta，DcMeta / ClusterMeta 始终新建；ShardMeta 仅在含容器 Keeper 时新建副本，
 * 其余子树（ShardMeta / RedisMeta / KeeperContainerMeta / SentinelMeta / RouteMeta 等）按引用共享，Console 缓存对象不被修改。
 * <p>
 * 只读内存、无 IO：依赖 {@link CrossDcKeeperContainerAddressService}，本机房读 ZK 缓存，远端读快照。
 */
@Component
public class CheckerKeeperIpFiller {

    private static final Logger logger = LoggerFactory.getLogger(CheckerKeeperIpFiller.class);

    @Autowired
    CrossDcKeeperContainerAddressService addressService;

    /**
     * 返回填充了容器 Keeper IP 的新 XpipeMeta。入参不被修改。
     * 解析异常不得导致接口失败：catch 后记录 ERROR 并返回未填充的副本。
     */
    public XpipeMeta fill(XpipeMeta source) {
        if (source == null) {
            return null;
        }
        XpipeMeta result = new XpipeMeta();
        result.mergeAttributes(source);
        source.getRedisCheckRules().values().forEach(result::addRedisCheckRule);

        for (DcMeta dcMeta : source.getDcs().values()) {
            DcMeta partDcMeta = new DcMeta(dcMeta.getId());
            partDcMeta.mergeAttributes(dcMeta);

            Map<String, String> hostnameToIp = resolveHostnameToIp(dcMeta.getId());
            List<String> unresolved = new ArrayList<>();

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                ClusterMeta partClusterMeta = new ClusterMeta(clusterMeta.getId());
                partClusterMeta.mergeAttributes(clusterMeta);
                clusterMeta.getSources().forEach(partClusterMeta::addSource);

                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    if (!hasContainerKeeper(shardMeta)) {
                        partClusterMeta.addShard(shardMeta);
                        continue;
                    }
                    partClusterMeta.addShard(copyShardWithResolvedIp(shardMeta, hostnameToIp, unresolved));
                }

                partDcMeta.addCluster(partClusterMeta);
            }

            dcMeta.getSentinels().values().forEach(partDcMeta::addSentinel);
            dcMeta.getKeeperContainers().forEach(partDcMeta::addKeeperContainer);
            dcMeta.getRoutes().forEach(partDcMeta::addRoute);
            dcMeta.getMetaServers().forEach(partDcMeta::addMetaServer);
            dcMeta.getAzs().forEach(partDcMeta::addAz);
            if (dcMeta.getZkServer() != null) {
                partDcMeta.setZkServer(dcMeta.getZkServer());
            }

            if (!unresolved.isEmpty()) {
                logger.warn("[fill][{}] unresolved container keepers: {}", dcMeta.getId(), unresolved);
            }

            result.addDc(partDcMeta);
        }

        return result;
    }

    private Map<String, String> resolveHostnameToIp(String dcId) {
        try {
            Map<String, String> map = addressService.getAllHostnameToIp(dcId);
            return map != null ? map : java.util.Collections.emptyMap();
        } catch (Throwable t) {
            logger.error("[fill][{}] failed to resolve hostname→IP, keepers in this dc will keep placeholder ip", dcId, t);
            return java.util.Collections.emptyMap();
        }
    }

    private boolean hasContainerKeeper(ShardMeta shardMeta) {
        for (KeeperMeta keeper : shardMeta.getKeepers()) {
            if (!StringUtil.isEmpty(keeper.getHostname())) {
                return true;
            }
        }
        return false;
    }

    private ShardMeta copyShardWithResolvedIp(ShardMeta source, Map<String, String> hostnameToIp, List<String> unresolved) {
        ShardMeta copy = new ShardMeta(source.getId());
        copy.mergeAttributes(source);

        for (KeeperMeta keeper : source.getKeepers()) {
            if (StringUtil.isEmpty(keeper.getHostname())) {
                copy.addKeeper(keeper);
                continue;
            }
            String ip = hostnameToIp.get(keeper.getHostname());
            if (StringUtil.isEmpty(ip)) {
                unresolved.add(keeper.getHostname() + ":" + keeper.getPort());
                copy.addKeeper(keeper);
                continue;
            }
            KeeperMeta keeperCopy = new KeeperMeta();
            keeperCopy.mergeAttributes(keeper);
            keeperCopy.setIp(ip);
            copy.addKeeper(keeperCopy);
        }

        for (RedisMeta redis : source.getRedises()) {
            copy.addRedis(redis);
        }
        source.getAppliers().forEach(copy::addApplier);

        return copy;
    }
}
