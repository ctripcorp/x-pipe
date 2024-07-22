package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.checker.ConsoleDcCheckerService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.KeeperStateModel;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.RedisRoleModel;
import com.ctrip.xpipe.redis.console.service.KeeperSessionService;
import com.ctrip.xpipe.redis.console.service.MetaServerSlotService;
import com.ctrip.xpipe.redis.console.service.RedisSessionService;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class FullLinkHealthCheckController extends AbstractController{

    @Autowired
    private ConsoleDcCheckerService consoleDcCheckerService;

    @Autowired
    private MetaServerSlotService metaServerSlotService;

    @Autowired
    private RedisSessionService redisSessionService;

    @Autowired
    private KeeperSessionService keeperSessionService;

    @RequestMapping(value = "/redis/role/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public List<RedisRoleModel> getShardAllRedisRole(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return redisSessionService.getShardAllRedisRole(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/keeper/states/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public List<KeeperStateModel> getShardAllKeeperState(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return keeperSessionService.getShardAllKeeperState(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/checker/groups/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public List<ShardCheckerHealthCheckModel> getShardAllCheckerGroupHealthCheck(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return consoleDcCheckerService.getShardAllCheckerGroupHealthCheck(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/meta/all/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public ShardAllMetaModel getShardAllMeta(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return metaServerSlotService.getShardAllMeta(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/shard/checker/group/health/check/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public List<ShardCheckerHealthCheckModel> getLocalDcShardAllCheckerGroupHealthCheck(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return consoleDcCheckerService.getLocalDcShardAllCheckerGroupHealthCheck(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/shard/meta/{dcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public ShardAllMetaModel getLocalDcShardAllMeta(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return metaServerSlotService.getLocalDcShardAllMeta(dcId, clusterId, shardId);
    }


}
