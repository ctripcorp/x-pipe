package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

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
  public List<KeeperBasicInfo> findBestKeepers(String dcName) {
    return findBestKeepers(dcName, RedisProtocol.REDIS_PORT_DEFAULT, (host, port) -> true);
  }

  @Override
  public List<KeeperBasicInfo> findBestKeepers(String dcName, int beginPort, BiPredicate keeperGood) {
    return findBestKeepers(dcName, beginPort, keeperGood, 2);
  }

  public List<KeeperBasicInfo> findBestKeepers(String dcName, int beginPort, BiPredicate<String, Integer> keeperGood,
      int returnCount) {

    List<KeepercontainerTbl> keeperCount = keepercontainerService.findKeeperCount(dcName);
    if (keeperCount.size() < returnCount) {
      throw new IllegalStateException("all keepers size:" + keeperCount + ", but we need:" + returnCount);
    }

    List<KeeperBasicInfo> result = new LinkedList<>();

    // find available port
    fillInResult(keeperCount, result, beginPort, keeperGood, returnCount);
    return result;

  }

  @Override
  public List<KeeperBasicInfo> findBestKeepersByCluster(String dcName, int beginPort,
      BiPredicate<String, Integer> keeperGood, ShardModel shardModel) {

    return findBestKeepersByCluster(dcName, beginPort, keeperGood, shardModel, 2);
  }

  private List<KeeperBasicInfo> findBestKeepersByCluster(String dcName, int beginPort,
      BiPredicate<String, Integer> keeperGood, ShardModel shardModel, int returnCount) {

    List<KeeperBasicInfo> result = new LinkedList<>();
    long clusterOrgId = getShardClusterOrgId(shardModel);
    List<KeepercontainerTbl> keeperCount = keepercontainerService.findKeeperCountByCluster(dcName, clusterOrgId);
    if (keeperCount.size() < returnCount) {
      throw new IllegalStateException(
          "Organization keepers size:" + keeperCount.size() + ", but we need:" + returnCount);
    }
    fillInResult(keeperCount, result, beginPort, keeperGood, returnCount);
    return result;
  }

  private long getShardClusterOrgId(ShardModel shardModel) {
    long clusterId = shardModel.getShardTbl().getClusterId();
    ClusterTbl clusterTbl = clusterService.find(clusterId);
    long clusterOrgId = clusterTbl.getClusterOrgId();
    return clusterOrgId;
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
