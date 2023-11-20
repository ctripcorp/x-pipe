package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;

import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public interface KeeperAdvancedService {

  List<KeeperBasicInfo> findBestKeepers(String dcName, int beginPort, BiPredicate<String, Integer> keeperGood,
      String clusterName);

  List<KeeperBasicInfo> findBestKeepers(String dcName, String clusterName);

  List<RedisTbl> getNewKeepers(String dcName, String clusterName, ShardModel shardModel, String srcKeeperContainerIp, String targetKeeperContainerIp);

  List<KeeperBasicInfo> findBestKeepersByKeeperContainer(String targetKeeperContainerIp, int beginPort,
                                                         BiPredicate<String, Integer> keeperGood, int returnCount);
}
