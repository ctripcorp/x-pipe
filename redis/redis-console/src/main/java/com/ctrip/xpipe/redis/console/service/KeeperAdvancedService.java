package com.ctrip.xpipe.redis.console.service;

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

}
