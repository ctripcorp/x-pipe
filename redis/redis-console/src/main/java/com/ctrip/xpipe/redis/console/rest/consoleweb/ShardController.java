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

  /**
   * 纯粹的shard和cluster之间的绑定关系，并且在绑定的dc下面都创建cluster-dc-shard关系 添加dc时，要copy shard信息
   */
  @RequestMapping(value = "/clusters/{clusterName}/shards", method = RequestMethod.POST)
  public ShardTbl createShard(@PathVariable String clusterName, @RequestBody ShardTbl shard) {
    return null;
  }

  //需要删除此shard已经在dc下面创建的实体shard？
  //释放掉shard的所有的相关资源，redis，keeper
  @RequestMapping(value = "/clusters/{clusterName}/shards/{shardName}", method = RequestMethod.DELETE)
  public void deleteShard(@PathVariable String clusterName, @PathVariable String shardName) {

  }


}
