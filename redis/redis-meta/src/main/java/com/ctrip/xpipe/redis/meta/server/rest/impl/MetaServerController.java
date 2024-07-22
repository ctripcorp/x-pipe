package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentShardMeta;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.meta.impl.CurrentOneWayShardMeta;
import com.ctrip.xpipe.redis.meta.server.meta.impl.CurrentShardKeeperMeta;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

import static com.ctrip.xpipe.cluster.ClusterType.ONE_WAY;

@RestController
@RequestMapping("/api/meta")
public class MetaServerController extends AbstractController {

    @Autowired
    private MetaServer currentMetaServer;

    @Autowired
    private DcMetaCache dcMetaCache;


    @RequestMapping(path = "/all/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ShardCurrentMetaModel getShardCurrentMeta(@PathVariable String clusterName, @PathVariable String shardName) {
        Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterName, shardName);
        long clusterDbId = clusterShard.getKey();
        if (ONE_WAY.equals(dcMetaCache.getClusterType(clusterDbId))) {
            return new ShardCurrentMetaModel()
                    .setShardDbId(clusterShard.getValue())
                    .setSurviveKeepers(currentMetaServer.getOneWaySurviveKeepers(clusterName, shardName))
                    .setKeeperMaster(currentMetaServer.getKeeperMaster(clusterName, shardName))
                    .setShardMeta(dcMetaCache.getClusterMeta(clusterDbId).getAllShards().get(shardName));
        }
        return null;
    }

}
