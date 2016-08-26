package com.ctrip.xpipe.redis.console.rest.consoleweb;


import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

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
public class ShardController {

  @Autowired
  private ClusterMetaService clusterService;
  @Autowired
  private ShardService shardService;

  @RequestMapping("/clusters/{clusterName}/dcs/{dcName}/shards")
  public List<ShardMeta> findShardMeta(@PathVariable String clusterName, @PathVariable String dcName){
    return new ArrayList<ShardMeta>(clusterService.getClusterMeta(dcName, clusterName).getShards().values());
  }

  @RequestMapping("/clusters/{clusterName}/shards")
  public List<ShardTbl> findShards(String clusterName) {
    return shardService.loadAllByClusterName(clusterName);
  }

  @RequestMapping(value = "/clusters/{clusterName}/shards", method = RequestMethod.POST)
  public ShardTbl createShard(@PathVariable String clusterName, @RequestBody ShardTbl shard) {
    return shardService.createShard(clusterName, shard);
  }

  @RequestMapping(value = "/clusters/{clusterName}/shards/{shardName}", method = RequestMethod.DELETE)
  public void deleteShard(@PathVariable String clusterName, @PathVariable String shardName) {

  }


}
