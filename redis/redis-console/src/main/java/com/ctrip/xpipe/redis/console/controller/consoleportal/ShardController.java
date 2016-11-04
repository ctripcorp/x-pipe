package com.ctrip.xpipe.redis.console.controller.consoleportal;


import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhangle
 */
@RestController
@RequestMapping("console")
public class ShardController extends AbstractConsoleController{

  @Autowired
  private ShardModelService shardModelService;
  @Autowired
  private ShardService shardService;

  @RequestMapping("/clusters/{clusterName}/dcs/{dcName}/shards")
  public List<ShardModel> findShardMetas(@PathVariable String clusterName, @PathVariable String dcName){
    return new ArrayList<ShardModel>(shardModelService.getAllShardModel(dcName, clusterName));
  }

  @RequestMapping("/clusters/{clusterName}/shards")
  public List<ShardTbl> findShards(@PathVariable String clusterName) {
	return valueOrEmptySet(ShardTbl.class, shardService.findAllByClusterName(clusterName));
  }
  
  @RequestMapping("/clusters/{clusterName}/dcs/{dcName}/shards/{shardName}")
  public ShardModel findShardMeta(@PathVariable String clusterName, @PathVariable String dcName, @PathVariable String shardName) {
	  return shardModelService.getShardModel(dcName, clusterName, shardName);
  }

  @RequestMapping(value = "/clusters/{clusterName}/shards", method = RequestMethod.POST)
  public ShardTbl createShard(@PathVariable String clusterName, @RequestBody ShardTbl shard) {
	logger.info("[Create Shard]{},{}",clusterName, shard);
    return shardService.createShard(clusterName, shard);
  }

  @RequestMapping(value = "/clusters/{clusterName}/shards/{shardName}", method = RequestMethod.DELETE)
  public void deleteShard(@PathVariable String clusterName, @PathVariable String shardName) {
	  logger.info("[Delete Shard]{},{}",clusterName, shardName);
	  shardService.deleteShard(clusterName, shardName);
  }


}
