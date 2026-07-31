package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionShardsCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedList;
import java.util.List;

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ShardUpdateController extends AbstractController {

    @Autowired
    private ShardService shardService;

    @PostMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/regions/" + REGION_NAME_PATH_VARIABLE + "/{shardName}")
    public RetMessage createRegionShard(@PathVariable String clusterName, @PathVariable String regionName,
        @PathVariable String shardName, @RequestBody(required = false) List<RedisCreateInfo> redisCreateInfos) {
        try {
            validateShardName(shardName);
            ShardService.RegionShardContext context = shardService.validateRegionCluster(clusterName, regionName);
            shardService.createRegionShard(context, shardName, redisCreateInfos);
            if (!CollectionUtils.isEmpty(redisCreateInfos)) {
                shardService.addRedises(context.getCluster(), shardName, redisCreateInfos);
                shardService.addKeepers(context.getCluster(), shardName, redisCreateInfos);
            }
        } catch (Exception e) {
            logger.error("[CreateRegionShard]Add Shard {} Failed", shardName, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
        return RetMessage.createSuccessMessage();
    }

    @PostMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/regions/" + REGION_NAME_PATH_VARIABLE)
    public RetMessage createRegionShards(@PathVariable String clusterName, @PathVariable String regionName,
        @RequestBody RegionShardsCreateInfo createInfo) {
        try {
            createInfo.check();
        } catch (CheckFailException e) {
            logger.error("[CreateRegionShards]Check Failed, Error: {}", e, e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        ShardService.RegionShardContext context;
        try {
            context = shardService.validateRegionCluster(clusterName, regionName);
        } catch (Exception e) {
            logger.error("[CreateRegionShards]Validate Failed", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        List<String> successShards = new LinkedList<>();
        List<String> failShards = new LinkedList<>();
        for (String shardName : createInfo.getShardNames()) {
            try {
                validateShardName(shardName);
                shardService.createRegionShard(context, shardName, null);
                successShards.add(shardName);
            } catch (Exception e) {
                logger.error("[CreateRegionShards]Add Shard {} Failed", shardName, e);
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

    private void validateShardName(String shardName) {
        if (shardName == null || !shardName.equals(shardName.trim())) {
            throw new BadRequestException("Shard name should not contain leading or trailing whitespace");
        }
    }

}
