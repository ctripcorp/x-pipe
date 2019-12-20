package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

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

    @Autowired
    private MetaCache metaCache;

    @RequestMapping(value = "/redises/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
    public List<String> getRedises(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {

        logger.info("[getRedises]{},{},{}", dcId, clusterId, shardId);

        List<String> result = new LinkedList<>();
        List<RedisTbl> redisTbls = null;
        try {
            redisTbls = redisService.findRedisesByDcClusterShard(outerDcToInnerDc(dcId), clusterId, shardId);
            redisTbls.forEach(new Consumer<RedisTbl>() {
                @Override
                public void accept(RedisTbl redisTbl) {
                    result.add(String.format("%s:%d", redisTbl.getRedisIp(), redisTbl.getRedisPort()));
                }
            });
        } catch (ResourceNotFoundException e) {
            logger.error("[getRedises]", e);
        }
        return result;
    }


    @RequestMapping(value = "/redises/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage addRedises(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId, @RequestBody List<String> redises) {

        logger.info("[addRedises]{},{},{}, {}", dcId, clusterId, shardId, redises);

        List<Pair<String, Integer>> redisAddresses = null;
        try {
            redisAddresses = getRedisAddresses(redises);
            redisService.insertRedises(outerDcToInnerDc(dcId), clusterId, shardId, redisAddresses);
            return RetMessage.createSuccessMessage();
        } catch (Exception e){
            logger.error("[addRedises]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

    }

    @RequestMapping(value = "/redises/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.DELETE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage deleteRedises(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId, @RequestBody List<String> redises) {

        logger.info("[deleteRedises]{},{},{}, {}", dcId, clusterId, shardId, redises);

        List<Pair<String, Integer>> redisAddresses = null;
        try {
            redisAddresses = getRedisAddresses(redises);
            redisService.deleteRedises(outerDcToInnerDc(dcId), clusterId, shardId, redisAddresses);
            return RetMessage.createSuccessMessage();
        } catch (Exception e){
            logger.error("[deleteRedises]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/redis/info/" + IP_ADDRESS_VARIABLE + "/{port}")
    public DcClusterShard getDcClusterShardByRedis(@PathVariable String ipAddress, @PathVariable int port) {
        try {
            HostPort hostPort = new HostPort(ipAddress, port);
            Pair<String, String> clusterShard = metaCache.findClusterShard(hostPort);
            String dcId = metaCache.getDc(hostPort);
            if (clusterShard == null) {
                return new DcClusterShard("", "", "");
            }
            return new DcClusterShard(dcId, clusterShard.getKey(), clusterShard.getValue());
        } catch (Exception e) {
            logger.error("[getClusterShardByRedis][{}:{}]", ipAddress, port, e);
        }
        return new DcClusterShard("", "", "");
    }

    private List<Pair<String,Integer>> getRedisAddresses(List<String> redises) {

        Set<Pair<String,Integer>> set = new HashSet<>();
        redises.forEach(new Consumer<String>() {
            @Override
            public void accept(String addr) {
                set.add(IpUtils.parseSingleAsPair(addr));
            }
        });
        return new LinkedList<>(set);
    }
}
