package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionShardsCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ShardCreateInfo;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ShardUpdateController extends AbstractController {

    @Autowired
    private DcService dcService;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private ShardService shardService;
    @Autowired
    private DcClusterShardService dcClusterShardService;
    @Autowired
    private SentinelBalanceService sentinelBalanceService;

//    @PostMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/regions/" + REGION_NAME_PATH_VARIABLE + "/{shardName}")
//    public RetMessage createRegionShard(@PathVariable String clusterName, @PathVariable String regionName,
//        @PathVariable String shardName) {
//        try {
//            shardService.createRegionShard(clusterName, regionName, shardName);
//        } catch (Exception e) {
//            logger.error("Create");
//            return RetMessage.createFailMessage(e.getMessage());
//        }
//        return RetMessage.createSuccessMessage();
//    }

    @PostMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/regions/" + REGION_NAME_PATH_VARIABLE)
    public RetMessage createRegionShards(@PathVariable String clusterName, @PathVariable String regionName,
        @RequestBody RegionShardsCreateInfo createInfo) {
        try {
            createInfo.check();
        } catch (CheckFailException e) {
            logger.error("[CreateRegionShards]Check Failed, Error: {}", e, e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        List<String> successShards = new LinkedList<>();
        List<String> failShards = new LinkedList<>();
        for (String shardName : createInfo.getShardNames()) {
            try {
                shardService.createRegionShard(clusterName, regionName, shardName);
                successShards.add(shardName);
            } catch (Exception e) {
                logger.error("[CreateRegionShards]Add Shard {} Failed, Error: {}", shardName, e, e);
                failShards.add(shardName);
            }
        }

        if (failShards.isEmpty()) {
            return RetMessage.createSuccessMessage();
        } else {
            StringBuilder sb = new StringBuilder();
            if (!successShards.isEmpty()) {
                sb.append(String.format("success shards:%s", joiner.join(successShards)));
            }
            sb.append(String.format("fail shards:%s", joiner.join(failShards)));
            return RetMessage.createFailMessage(sb.toString());
        }
    }

}
