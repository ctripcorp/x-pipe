package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Component
public class CrossDcSentinelHellosCollector extends DefaultSentinelHelloCollector {

    @Autowired
    private CheckerConfig checkerConfig;

    @Override
    protected String getSentinelMonitorName(RedisInstanceInfo info) {
        return SentinelUtil.getSentinelMonitorName(info.getClusterId(), info.getShardId(), checkerConfig.crossDcSentinelMonitorNameSuffix());
    }

    @Override
    protected Set<HostPort> getSentinels(RedisInstanceInfo info) {

        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();

        if (!checkDcClusterShardExist(dcId, clusterId, shardId)) {
            return Collections.emptySet();
        }

        DcMeta instanceDcMeta = metaCache.getXpipeMeta().getDcs().get(dcId);
        ShardMeta shardMetaInCurrentDc = instanceDcMeta.getClusters().get(clusterId).getShards().get(shardId);
        long sentinelGroupId = shardMetaInCurrentDc.getSentinelId();
        Set<HostPort> crossDcSentinels = new HashSet<>();
        metaCache.getXpipeMeta().getDcs().forEach((dc, dcMeta) -> {
            SentinelMeta sentinelMeta = dcMeta.getSentinels().get(sentinelGroupId);
            if (sentinelMeta != null)
                crossDcSentinels.addAll(IpUtils.parseAsHostPorts(sentinelMeta.getAddress()));
        });

        return crossDcSentinels;
    }

    @Override
    protected boolean isKeeperOrDead(HostPort hostPort) {
        // no keeper for cross dc cluster
        return false;
    }

    @Override
    protected boolean isHelloMasterInWrongDc(SentinelHello hello) {
//      master of cross dc cluster may in any dc
        return false;
    }


    private boolean checkDcClusterShardExist(String dcId, String clusterId, String shardId) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        return null != xpipeMeta
                && xpipeMeta.getDcs().containsKey(dcId)
                && xpipeMeta.getDcs().get(dcId).getClusters().containsKey(clusterId)
                && xpipeMeta.getDcs().get(dcId).getClusters().get(clusterId).getShards().containsKey(shardId);
    }

}
