package com.ctrip.xpipe.redis.console.service.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
@Component
public class DefaultKeeperAdvancedService extends AbstractConsoleService<RedisTblDao> implements KeeperAdvancedService {

  @Autowired
  private KeepercontainerService keepercontainerService;

  @Autowired
  private RedisService redisService;

  @Autowired
  private ClusterService clusterService;

  @Override
  public List<KeeperBasicInfo> findBestKeepers(String dcName, String clusterName) {
    return findBestKeepers(dcName, RedisProtocol.REDIS_PORT_DEFAULT, (host, port) -> true, clusterName);
  }

  @Override
  public List<KeeperBasicInfo> findBestKeepers(String dcName, int beginPort, BiPredicate keeperGood,
      String clusterName) {
    return findBestKeepers(dcName, beginPort, keeperGood, clusterName, 2);
  }

  public List<KeeperBasicInfo> findBestKeepers(String dcName, int beginPort, BiPredicate<String, Integer> keeperGood,
      String clusterName, int returnCount) {

    List<KeeperBasicInfo> result = new LinkedList<>();

    List<KeepercontainerTbl> keepercontainerTbls =
        keepercontainerService.findBestKeeperContainersByDcCluster(dcName, clusterName);
    if (keepercontainerTbls.size() < returnCount) {
      throw new IllegalStateException(
          "Organization keepers size:" + keepercontainerTbls.size() + ", but we need:" + returnCount);
    }

    fillInResult(keepercontainerTbls, result, beginPort, keeperGood, returnCount);
    return result;

  }

  private void fillInResult(List<KeepercontainerTbl> keeperCount, List<KeeperBasicInfo> result, int beginPort,
      BiPredicate<String, Integer> keeperGood, int returnCount) {
    // find available port
    for (int i = 0; i < returnCount; i++) {

      KeepercontainerTbl keepercontainerTbl = keeperCount.get(i);

      KeeperBasicInfo keeperSelected = new KeeperBasicInfo();

      keeperSelected.setKeeperContainerId(keepercontainerTbl.getKeepercontainerId());
      keeperSelected.setHost(keepercontainerTbl.getKeepercontainerIp());

      int port = findAvailablePort(keepercontainerTbl, beginPort, keeperGood, result);

      keeperSelected.setPort(port);
      result.add(keeperSelected);
    }
  }

  private int findAvailablePort(KeepercontainerTbl keepercontainerTbl, int beginPort,
      BiPredicate<String, Integer> keeperGood, List<KeeperBasicInfo> result) {

    int port = beginPort;
    String ip = keepercontainerTbl.getKeepercontainerIp();

    for (;; port++) {

      if (alreadySelected(ip, port, result)) {
        continue;
      }

      if (!(keeperGood.test(ip, port))) {
        continue;
      }
      if (existInDb(ip, port)) {
        continue;
      }

      break;
    }

    return port;
  }

  private boolean alreadySelected(String ip, int port, List<KeeperBasicInfo> result) {

    for (KeeperBasicInfo keeperSelected : result) {
      if (keeperSelected.getHost().equalsIgnoreCase(ip) && keeperSelected.getPort() == port) {
        return true;
      }
    }
    return false;
  }

  private boolean existInDb(String keepercontainerIp, int port) {
    return redisService.findWithIpPort(keepercontainerIp, port) != null;
  }

}
