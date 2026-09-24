package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.cluster.ClusterType.ONE_WAY;

@RestController
@RequestMapping("/api/meta")
public class MetaServerController extends AbstractController {

    @Autowired
    private MetaServer currentMetaServer;

    @Autowired
    private DcMetaCache dcMetaCache;

    @Autowired
    private CurrentDcKeeperContainerAddressService keeperContainerAddressService;

    @RequestMapping(path = "/all/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ShardCurrentMetaModel getShardCurrentMeta(@PathVariable String clusterName, @PathVariable String shardName) {
        Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterName, shardName);
        long clusterDbId = clusterShard.getKey();
        if (ONE_WAY.equals(dcMetaCache.getClusterType(clusterDbId))) {
            List<KeeperMeta> surviveKeepers = currentMetaServer.getOneWaySurviveKeepers(clusterName, shardName);
            fillContainerKeeperAddress(clusterDbId, clusterShard.getValue(), surviveKeepers);
            return new ShardCurrentMetaModel()
                    .setShardDbId(clusterShard.getValue())
                    .setSurviveKeepers(surviveKeepers)
                    .setKeeperMaster(currentMetaServer.getKeeperMaster(clusterName, shardName))
                    .setShardMeta(dcMetaCache.getClusterMeta(clusterDbId).getAllShards().get(shardName));
        }
        return null;
    }

    /**
     * surviveKeepers 来自 ZK 注册态，只有 ip/port/active/keeperContainerId，
     * 而 ZK 里的 ip 就是 console 下发的占位值，对容器 keeper 没有意义。
     * 这里按 keeperContainerId 从 DcMetaCache 取回 hostname，再用本机房缓存反查出真实 IP 一并返回，
     * 让 console 侧不必再自行拼接定位信息。
     * <p>
     * surviveKeepers 是 getOneWaySurviveKeepers 新建的对象，可以安全改写；
     * shardMeta 是 DcMetaCache 内的共享实例，不在这里改动
     */
    private void fillContainerKeeperAddress(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviveKeepers) {
        if (null == surviveKeepers || surviveKeepers.isEmpty()) {
            return;
        }

        List<KeeperMeta> metaKeepers = dcMetaCache.getShardKeepers(clusterDbId, shardDbId);
        if (null == metaKeepers || metaKeepers.isEmpty()) {
            return;
        }

        Map<String, String> hostnameToIp = null;
        for (KeeperMeta surviveKeeper : surviveKeepers) {
            KeeperMeta metaKeeper = findMatching(metaKeepers, surviveKeeper);
            if (null == metaKeeper || StringUtil.isEmpty(metaKeeper.getHostname())) {
                continue;
            }
            surviveKeeper.setHostname(metaKeeper.getHostname());

            // 存在容器 keeper 时才取映射，纯物理机分片不产生额外开销
            if (null == hostnameToIp) {
                hostnameToIp = keeperContainerAddressService.getAllHostnameToIp();
            }
            String realIp = hostnameToIp.get(metaKeeper.getHostname());
            if (!StringUtil.isEmpty(realIp)) {
                surviveKeeper.setIp(realIp);
            } else {
                logger.warn("[fillContainerKeeperAddress][cluster_{},shard_{}] no alive ip for hostname: {}",
                        clusterDbId, shardDbId, metaKeeper.getHostname());
            }
        }
    }

    /**
     * 优先按 keeperContainerId 匹配：容器 keeper 的 ip 是占位值，
     * 同一分片内多个 pod 可能 ip 和端口都相同，只按 ip:port 匹配会错配
     */
    private KeeperMeta findMatching(List<KeeperMeta> metaKeepers, KeeperMeta surviveKeeper) {
        Long containerId = surviveKeeper.getKeeperContainerId();
        if (null != containerId && containerId > 0) {
            for (KeeperMeta metaKeeper : metaKeepers) {
                if (containerId.equals(metaKeeper.getKeeperContainerId())
                        && metaKeeper.getPort() == surviveKeeper.getPort()) {
                    return metaKeeper;
                }
            }
        }
        // keeperContainerId 缺失（enrich 时未匹配到）时退回 ip:port
        for (KeeperMeta metaKeeper : metaKeepers) {
            if (MetaUtils.same(metaKeeper, surviveKeeper)) {
                return metaKeeper;
            }
        }
        return null;
    }
}
