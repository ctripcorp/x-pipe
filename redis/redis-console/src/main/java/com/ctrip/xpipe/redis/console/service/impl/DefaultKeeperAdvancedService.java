package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.StringUtil;
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
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;

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

  @Autowired
  private AzService azService;

  @Autowired
  private ConsoleConfig consoleConfig;

  @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
  ExecutorService executor;

  @Override
  public List<KeeperBasicInfo> findBestKeepers(String dcName, String clusterName) {
    return findBestKeepers(dcName, KEEPER_PORT_DEFAULT, (host, port) -> true, clusterName);
  }

  @Override
  public List<KeeperBasicInfo> findBestKeepers(String dcName, int beginPort, BiPredicate keeperGood,
      String clusterName) {
    return findBestKeepers(dcName, beginPort, keeperGood, clusterName, 2);
  }

  public List<KeeperBasicInfo> findBestKeepers(String dcName, int beginPort, BiPredicate<String, Integer> keeperGood,
      String clusterName, int returnCount) {

    List<KeeperBasicInfo> result = new LinkedList<>();

    // D36: disk-type before AZ; pool query itself does neither.
    List<KeepercontainerTbl> keepercontainerTbls =
        filterByDiskType(keeperContainerService.findBestKeeperContainersByDcCluster(dcName, clusterName));
    keepercontainerTbls = keeperContainerService.filterKeeperContainersByAz(keepercontainerTbls, dcName);
    if (keepercontainerTbls.size() < returnCount) {
      throw new IllegalStateException(
          "Organization keepers size:" + keepercontainerTbls.size() + ", but we need:" + returnCount);
    }

    fillInResult(keepercontainerTbls, result, beginPort, keeperGood, returnCount);
    return result;
  }

  private List<KeepercontainerTbl> filterByDiskType(List<KeepercontainerTbl> keepercontainerTbls) {
    String policy = consoleConfig.getKeeperAutoSelectDiskType();
    if (policy == null || "ANY".equalsIgnoreCase(policy)) {
      return keepercontainerTbls;
    }
    boolean wantTfs = "TFS".equalsIgnoreCase(policy);
    return keepercontainerTbls.stream()
        .filter(kc -> KeeperDiskTypeUtils.isTfs(kc.getKeepercontainerDiskType()) == wantTfs)
        .collect(Collectors.toList());
  }

  @Override
  public List<RedisTbl> getNewKeepers(String dcName, String clusterName, ShardModel shardModel, long srcKeeperContainerId, long targetKeeperContainerId) {
    List<RedisTbl> newKeepers = new ArrayList<>();
    logger.debug("[migrateKeepers] origin keepers {} from cluster:{}, dc:{}, shard:{}",shardModel.getKeepers(), clusterName, dcName, shardModel.getShardTbl().getShardName());
    // Replacement semantics: the new keeper inherits the election weight of the keeper it replaces
    // (the one on srcKeeperContainerIp). Do NOT fall back to the per-medium config default here —
    // during grayscale the TFS default is still 0, which would mislabel a keeper migrated onto BM.
    int replacedKeeperPriority = 0;
    for (RedisTbl keeper : shardModel.getKeepers()) {
      if (keeper.getKeepercontainerId() != srcKeeperContainerId) {
        newKeepers.add(keeper);
      } else {
        replacedKeeperPriority = keeper.getKeeperPriority();
      }
    }

    if (newKeepers.isEmpty()) {
      logger.warn("[migrateKeepers] unexpected keepers {} from cluster:{}, dc:{}, shard:{}", 
              newKeepers, clusterName, dcName, shardModel.getShardTbl().getShardName());
      return newKeepers;
    }

    long alreadyUsedAzId = keeperContainerService.find(newKeepers.get(0).getKeepercontainerId()).getAzId();
    
    // 当 targetKeeperContainerId 为 0 时，自动选择最佳 keeper container（兼容老逻辑）
    List<KeeperBasicInfo> bestKeepers;
    if (targetKeeperContainerId == 0) {
      bestKeepers = findBestKeepers(dcName, KEEPER_PORT_DEFAULT, (ip, port) -> true, clusterName);
    } else {
      bestKeepers = findBestKeepersByKeeperContainerId(targetKeeperContainerId, KEEPER_PORT_DEFAULT, (ip, port) -> true, 1);
    }
    
    logger.debug("[migrateKeepers] best keepers {} from cluster:{}, dc:{}, shard:{}, targetContainerId:{}, srcContainerId:{}", 
            bestKeepers, clusterName, dcName, shardModel.getShardTbl().getShardName(), targetKeeperContainerId, srcKeeperContainerId);
    for (KeeperBasicInfo keeperSelected : bestKeepers) {
      logger.debug("[migrateKeepers] keeperSelected {}, result:{}", keeperSelected, keeperSelected.getKeeperContainerId() == srcKeeperContainerId);
      if (keeperSelected.getKeeperContainerId() != srcKeeperContainerId
              && keeperSelected.getKeeperContainerId() != newKeepers.get(0).getKeepercontainerId()
              && isDifferentAz(keeperSelected, alreadyUsedAzId, dcName)) {
        newKeepers.add(new RedisTbl().setKeepercontainerId(keeperSelected.getKeeperContainerId())
                .setRedisIp(keeperSelected.getHost())
                .setHostName(null == keeperSelected.getHostName() ? "" : keeperSelected.getHostName())
                .setRedisPort(keeperSelected.getPort())
                .setRedisRole(XPipeConsoleConstant.ROLE_KEEPER)
                .setKeeperPriority(replacedKeeperPriority));
        break;
      }
    }
    logger.debug("[migrateKeepers] new keepers {} from cluster:{}, dc:{}, shard:{}", 
            newKeepers, clusterName, dcName, shardModel.getShardTbl().getShardName());
    return newKeepers;
  }
  
  private List<KeeperBasicInfo> findBestKeepersByKeeperContainerId(long targetKeeperContainerId, int beginPort, BiPredicate<String, Integer> keeperGood, int returnCount) {
    List<KeeperBasicInfo> result = new LinkedList<>();
    KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(targetKeeperContainerId);
    if (keepercontainerTbl == null) {
      logger.warn("[findBestKeepersByKeeperContainerId] keepercontainer not found for id: {}", targetKeeperContainerId);
      return result;
    }
    List<KeepercontainerTbl> keepercontainerTbls = new LinkedList<>();
    keepercontainerTbls.add(keepercontainerTbl);
    fillInResult(keepercontainerTbls, result, beginPort, keeperGood, returnCount);
    return result;
  }

  @Override
  public List<RedisTbl> getSwitchMaterNewKeepers(ShardModel shardModel) {
    List<RedisTbl> newKeepers = new ArrayList<>();
    for (RedisTbl keeper : shardModel.getKeepers()) {
      keeper.setMaster(!keeper.isMaster());
      newKeepers.add(keeper);
    }
    return newKeepers;
  }

  private boolean isDifferentAz(KeeperBasicInfo keeperSelected, long alreadyUsedAzId, String dcName) {
    if (alreadyUsedAzId == 0 || !azService.isDcSupportMultiAz(dcName))  return true;
    long newAzId = keeperContainerService.find(keeperSelected.getKeeperContainerId()).getAzId();
    return newAzId != alreadyUsedAzId;
  }



  private void fillInResult(List<KeepercontainerTbl> keeperCount, List<KeeperBasicInfo> result, int beginPort,
      BiPredicate<String, Integer> keeperGood, int returnCount) {

    Map<Long, Set<Integer>> usedPortsByContainer = getUsedPortsByKeeperContainerId(keeperCount);
    // find available port
    for (int i = 0; i < returnCount; i++) {

      KeepercontainerTbl keepercontainerTbl = keeperCount.get(i);

      KeeperBasicInfo keeperSelected = new KeeperBasicInfo();

      keeperSelected.setKeeperContainerId(keepercontainerTbl.getKeepercontainerId());
      // 物理机为真实 IP；容器为登记时的占位值（DB 默认 0.0.0.0），真实地址等消费方经 ZK 反查
      keeperSelected.setHost(keepercontainerTbl.getKeepercontainerIp());
      keeperSelected.setHostName(hostNameOf(keepercontainerTbl));

      int port = findAvailablePort(keepercontainerTbl, beginPort, keeperGood, result, usedPortsByContainer);

      keeperSelected.setPort(port);
      result.add(keeperSelected);
    }
  }

  @VisibleForTesting
  int findAvailablePort(KeepercontainerTbl keepercontainerTbl, int beginPort,
                        BiPredicate<String, Integer> keeperGood, List<KeeperBasicInfo> result, Map<Long, Set<Integer>> usedPortsByContainer) {

    int port = beginPort;
    long keeperContainerId = keepercontainerTbl.getKeepercontainerId();

    Set<Integer> existingPorts = usedPortsByContainer.get(keeperContainerId);

    for (;; port++) {

      if (alreadySelected(keeperContainerId, port, result)) {
        continue;
      }

      String containerKey = containerKey(keepercontainerTbl);
      if (!(keeperGood.test(containerKey, port))) {
        continue;
      }
      if (existingPorts.contains(port)) {
        continue;
      }

      break;
    }

    return port;
  }

  private boolean alreadySelected(long keeperContainerId, int port, List<KeeperBasicInfo> result) {

    for (KeeperBasicInfo keeperSelected : result) {
      if (keeperSelected.getKeeperContainerId() == keeperContainerId && keeperSelected.getPort() == port) {
        return true;
      }
    }
    return false;
  }

  /**
   * 端口记账的键：物理机容器用 IP，pod 容器用 hostname。
   * <p>
   * 该键仅用于 keeperGood 断言，让调用方区分 pod。
   * 端口记账已统一改用 keepercontainer_id。
   */
  private String containerKey(KeepercontainerTbl keepercontainerTbl) {
    return isPodContainer(keepercontainerTbl)
            ? keepercontainerTbl.getKeepercontainerHostName()
            : keepercontainerTbl.getKeepercontainerIp();
  }

  private String containerKeyOf(KeeperBasicInfo keeperSelected) {
    return keeperSelected.isPodKeeper() ? keeperSelected.getHostName() : keeperSelected.getHost();
  }

  /**
   * 判定容器形态只看 hostname 是否有值，不看 IP 是否为 0.0.0.0：hostname 才是容器的身份，
   * 而 IP 对容器只是个占位。console 侧所有"物理机还是容器"的分支都用这一个依据。
   */
  private boolean isPodContainer(KeepercontainerTbl keepercontainerTbl) {
    return !StringUtil.isEmpty(keepercontainerTbl.getKeepercontainerHostName());
  }

  private String hostNameOf(KeepercontainerTbl keepercontainerTbl) {
    String hostName = keepercontainerTbl.getKeepercontainerHostName();
    // 与 DB 的 NOT NULL DEFAULT '' 对齐，避免 null 流到落库环节
    return null == hostName ? "" : hostName;
  }

  private Map<Long, Set<Integer>> getUsedPortsByKeeperContainerId(List<KeepercontainerTbl> keeperCount) {

    Map<Long, Set<Integer>> map = Maps.newHashMap();
    keeperCount.forEach(kc -> map.putIfAbsent(kc.getKeepercontainerId(), new HashSet<Integer>()));

    List<Future> futures = new ArrayList<>(map.size());
    for(Map.Entry<Long, Set<Integer>> entry : map.entrySet()) {

      long keeperContainerId = entry.getKey();
      Set<Integer> existingPorts = entry.getValue();
      Future future = executor.submit(new Runnable() {
        @Override
        public void run() {
          List<RedisTbl> redisOnSameContainer = redisService.findAllKeepersByKeeperContainerId(keeperContainerId);
          redisOnSameContainer.forEach(redisTbl -> existingPorts.add(redisTbl.getRedisPort()));
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
        return getUsedPortsByKeeperContainerId(keeperCount);
      }
    }
    return map;
  }
}
