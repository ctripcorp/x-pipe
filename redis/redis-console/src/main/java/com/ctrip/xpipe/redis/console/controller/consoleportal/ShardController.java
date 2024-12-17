package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ShardListModel;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhangle
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ShardController extends AbstractConsoleController {

    @Autowired
    private ShardModelService shardModelService;
    @Autowired
    private ShardService shardService;

    @RequestMapping("/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}/shards")
    public List<ShardModel> findShardModels(@PathVariable String clusterName, @PathVariable String dcName) {
        return new ArrayList<>(shardModelService.getAllShardModel(dcName, clusterName));
    }

    @RequestMapping("/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards")
    public List<ShardTbl> findShards(@PathVariable String clusterName) {
        return valueOrEmptySet(ShardTbl.class, shardService.findAllByClusterName(clusterName));
    }

    @RequestMapping("/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}/shards/" + SHARD_NAME_PATH_VARIABLE)
    public ShardModel findShardModel(@PathVariable String clusterName, @PathVariable String dcName, @PathVariable String shardName) {
        return shardModelService.getShardModel(dcName, clusterName, shardName, false, null);
    }

    @RequestMapping("/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/src-dc/{srcDcName}/to-dc/{toDcName}/shards/" + SHARD_NAME_PATH_VARIABLE)
    public ShardModel findSourceShardModel(@PathVariable String clusterName, @PathVariable String srcDcName,
                                           @PathVariable String toDcName, @PathVariable String shardName) {
        return shardModelService.getSourceShardModel(clusterName, srcDcName, toDcName, shardName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards", method = RequestMethod.POST)
    public ShardTbl createShard(@PathVariable String clusterName, @RequestBody ShardModel shard) {
        logger.info("[Create Shard]{},{}", clusterName, shard);
        return shardService.createShard(clusterName, shard.getShardTbl(), shard.getSentinels());
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.DELETE)
    public void deleteShard(@PathVariable String clusterName, @PathVariable String shardName) {
        logger.info("[Delete Shard]{},{}", clusterName, shardName);
        shardService.deleteShard(clusterName, shardName);
    }

    @RequestMapping(value = "/shards/unhealthy", method = RequestMethod.GET)
    public List<ShardListModel> findAllUnhealthyShards() {
        return valueOrEmptySet(ShardListModel.class, shardService.findAllUnhealthy());
    }

    @RequestMapping(value = "/shards/allById/{id}", method = RequestMethod.GET)
    public RetMessage findAllShardsById(@PathVariable long id) {
        try {
            List<ShardListModel> result = new ArrayList<>();
            result.add(shardService.findByReplId(id));
            return GenericRetMessage.createGenericRetMessage(result);
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/shards/allByName/{shardName}", method = RequestMethod.GET)
    public RetMessage findAllShardsByShardName(@PathVariable String shardName) {
        try {
            return GenericRetMessage.createGenericRetMessage(shardService.findAllByShardName(shardName));
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

}
