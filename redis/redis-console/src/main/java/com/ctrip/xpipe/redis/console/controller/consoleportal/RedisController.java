package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author zhangle 16/8/24
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class RedisController extends AbstractConsoleController {
    @Autowired
    private RedisService redisService;

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public void updateRedises(@PathVariable String clusterName, @PathVariable String dcName,
                              @PathVariable String shardName, @RequestBody(required = false) ShardModel shardModel) {
        try {
            if (null != shardModel) {
                logger.info("[Update Redises][construct]{},{},{},{}", clusterName, dcName, shardName, shardModel);
                redisService.updateRedises(dcName, clusterName, shardName, shardModel);
                logger.info("[Update Redises][success]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            } else {
                logger.error("[Update Redises][Null ShardModel]{},{},{}", clusterName, dcName, shardName);
            }
        } catch (Exception e) {
            logger.error("[Update Redises][failed]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            throw e;
        }
    }
}
