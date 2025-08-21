package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class RedisApiController extends AbstractConsoleController {

    @Autowired
    private RedisService redisService;

    @RequestMapping(value = "/redis/{dcClusterShardId}", method = RequestMethod.GET)
    List<RedisTbl> findAllByDcClusterShard(@PathVariable long dcClusterShardId) {
        return redisService.findAllByDcClusterShard(dcClusterShardId);
    }

    @RequestMapping(value = "/redis/updateBatchKeeperActive", method = RequestMethod.POST)
    RetMessage updateBatchKeeperActive(@RequestBody List<RedisTbl> redises) {
        try {
            redisService.updateBatchKeeperActive(redises);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/redis/insert", method = RequestMethod.POST)
    RetMessage insert(@RequestBody RedisCreateInfo redisCreateInfo) {
        try {
            redisService.insertRedises(redisCreateInfo.getDcId(),
                    redisCreateInfo.getClusterId(), redisCreateInfo.getShardName(),
                    redisCreateInfo.getRedisAddresses());
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

}
