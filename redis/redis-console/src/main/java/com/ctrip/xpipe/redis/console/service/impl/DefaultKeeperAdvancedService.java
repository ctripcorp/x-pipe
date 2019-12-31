package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
@Component
public class DefaultKeeperAdvancedService extends AbstractConsoleService<RedisTblDao> implements KeeperAdvancedService {

  @Autowired
  private KeeperContainerService keeperContainerService;

  @Autowired
  private RedisService redisService;

  @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
  ExecutorService executor;

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
        keeperContainerService.findBestKeeperContainersByDcCluster(dcName, clusterName);
    if (keepercontainerTbls.size() < returnCount) {
      throw new IllegalStateException(
          "Organization keepers size:" + keepercontainerTbls.size() + ", but we need:" + returnCount);
    }

    fillInResult(keepercontainerTbls, result, beginPort, keeperGood, returnCount);
    return result;

  }


  private void fillInResult(List<KeepercontainerTbl> keeperCount, List<KeeperBasicInfo> result, int beginPort,
      BiPredicate<String, Integer> keeperGood, int returnCount) {

    Map<String, Set<Integer>> ipAndPorts = getIpAndPortsWithSameIpAsKC(keeperCount);
    // find available port
    for (int i = 0; i < returnCount; i++) {

      KeepercontainerTbl keepercontainerTbl = keeperCount.get(i);

      KeeperBasicInfo keeperSelected = new KeeperBasicInfo();

      keeperSelected.setKeeperContainerId(keepercontainerTbl.getKeepercontainerId());
      keeperSelected.setHost(keepercontainerTbl.getKeepercontainerIp());

      int port = findAvailablePort(keepercontainerTbl, beginPort, keeperGood, result, ipAndPorts);

      keeperSelected.setPort(port);
      result.add(keeperSelected);
    }
  }

  @VisibleForTesting
  int findAvailablePort(KeepercontainerTbl keepercontainerTbl, int beginPort,
                        BiPredicate<String, Integer> keeperGood, List<KeeperBasicInfo> result, Map<String, Set<Integer>> ipAndPorts) {

    int port = beginPort;
    String ip = keepercontainerTbl.getKeepercontainerIp();

    Set<Integer> existingPorts = ipAndPorts.get(ip);

    for (;; port++) {

      if (alreadySelected(ip, port, result)) {
        continue;
      }

      if (!(keeperGood.test(ip, port))) {
        continue;
      }
      if (existingPorts.contains(port)) {
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


  private Map<String, Set<Integer>> getIpAndPortsWithSameIpAsKC(List<KeepercontainerTbl> keeperCount) {

    Map<String, Set<Integer>> map = Maps.newHashMap();
    keeperCount.forEach(kc -> map.putIfAbsent(kc.getKeepercontainerIp(), new HashSet<Integer>()));

    List<Future> futures = new ArrayList<>(map.size());
    for(Map.Entry<String, Set<Integer>> entry : map.entrySet()) {

      String ip = entry.getKey();
      Set<Integer> existingPorts = entry.getValue();
      Future future = executor.submit(new Runnable() {
        @Override
        public void run() {
          List<RedisTbl> redisWithSameIP = redisService.findAllRedisWithSameIP(ip);
          redisWithSameIP.forEach(redisTbl -> existingPorts.add(redisTbl.getRedisPort()));
        }
      });
      futures.add(future);
    }
    for(Future future : futures) {
      try {
        future.get();
      } catch (InterruptedException ignore) {
      } catch (ExecutionException e) {
        for(Future futureToCancel : futures) {
          if(!futureToCancel.isDone() || !futureToCancel.isCancelled()) {
            futureToCancel.cancel(true);
          }
        }
        return getIpAndPortsWithSameIpAsKC(keeperCount);
      }
    }
    return map;
  }
}
