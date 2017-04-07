package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class RedisUpdateController extends AbstractConsoleController{

    @Autowired
    private RedisService redisService;

    @RequestMapping(value = "/redises/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public List<String> getRedises(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {

        logger.info("[getRedises]{},{},{}", dcId, clusterId, shardId);

        List<String> result = new LinkedList<>();

        List<RedisTbl> redisTbls = redisService.findAllByDcClusterShard(dcId, clusterId, shardId);
        for(RedisTbl redisTbl : redisTbls){
            if(redisTbl.getRedisRole().equalsIgnoreCase(XpipeConsoleConstant.ROLE_REDIS)){
                result.add(String.format("%s:%d", redisTbl.getRedisIp(), redisTbl.getRedisPort()));
            }
        }
        return result;
    }

    @RequestMapping(value = "/redises/{dcId}/{clusterId}/{shardId}", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage addRedises(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId, @RequestBody List<String> redises) {

        logger.info("[addRedises]{},{},{}, {}", dcId, clusterId, shardId, redises);
        return new RetMessage(0);
    }

    @RequestMapping(value = "/redises/{dcId}/{clusterId}/{shardId}", method = RequestMethod.DELETE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage deleteRedises(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId, @RequestBody List<String> redises) {

        logger.info("[deleteRedises]{},{},{}, {}", dcId, clusterId, shardId, redises);
        return new RetMessage(0);
    }

}
