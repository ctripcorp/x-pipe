package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.ObjectUtils;
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
  public List<RedisTbl> getNewKeepers(String dcName, String clusterName, ShardModel shardModel, String srcKeeperContainerIp, String targetKeeperContainerIp) {
    List<RedisTbl> newKeepers = new ArrayList<>();
    logger.debug("[migrateKeepers] origin keepers {} from cluster:{}, dc:{}, shard:{}",shardModel.getKeepers(), clusterName, dcName, shardModel.getShardTbl().getShardName());
    // Replacement semantics: the new keeper inherits the election weight of the keeper it replaces
    // (the one on srcKeeperContainerIp). Do NOT fall back to the per-medium config default here —
    // during grayscale the TFS default is still 0, which would mislabel a keeper migrated onto BM.
    int replacedKeeperPriority = 0;
    for (RedisTbl keeper : shardModel.getKeepers()) {
      if (!ObjectUtils.equals(keeper.getRedisIp(), srcKeeperContainerIp)) {
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
    List<KeeperBasicInfo> bestKeepers = findBestKeepers(clusterName, dcName, targetKeeperContainerIp);
    logger.debug("[migrateKeepers] best keepers {} from cluster:{}, dc:{}, shard:{}, targetKeeperContainerIp:{}, srcKeeperContainerIp:{}",
            bestKeepers, clusterName, dcName, shardModel.getShardTbl().getShardName(), targetKeeperContainerIp, srcKeeperContainerIp);
    for (KeeperBasicInfo keeperSelected : bestKeepers) {
      logger.debug("[migrateKeepers] keeperSelected {} , result:{}",keeperSelected, ObjectUtils.equals(keeperSelected.getHost(), srcKeeperContainerIp));
      if (!ObjectUtils.equals(keeperSelected.getHost(), srcKeeperContainerIp)
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

  private List<KeeperBasicInfo> findBestKeepers(String clusterName, String dcName, String targetKeeperContainerIp) {
    if (targetKeeperContainerIp == null ) {
      return findBestKeepers(dcName, KEEPER_PORT_DEFAULT, (ip, port) -> true, clusterName);
    } else {
      return findBestKeepersByKeeperContainer(targetKeeperContainerIp, KEEPER_PORT_DEFAULT, (ip, port) -> true, 1);
    }
  }

  @Override
  public List<KeeperBasicInfo> findBestKeepersByKeeperContainer(String targetKeeperContainerIp,
                                              int beginPort, BiPredicate<String, Integer> keeperGood, int returnCount) {
    List<KeeperBasicInfo> result = new LinkedList<>();
    List<KeepercontainerTbl> keepercontainerTbls = new LinkedList<>();
    keepercontainerTbls.add(keeperContainerService.find(targetKeeperContainerIp));
    fillInResult(keepercontainerTbls, result, beginPort, keeperGood, returnCount);
    return result;
  }


  private void fillInResult(List<KeepercontainerTbl> keeperCount, List<KeeperBasicInfo> result, int beginPort,
      BiPredicate<String, Integer> keeperGood, int returnCount) {

    Map<String, Set<Integer>> usedPortsByContainer = getUsedPortsByContainerKey(keeperCount);
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
                        BiPredicate<String, Integer> keeperGood, List<KeeperBasicInfo> result, Map<String, Set<Integer>> usedPortsByContainer) {

    int port = beginPort;
    String containerKey = containerKey(keepercontainerTbl);

    Set<Integer> existingPorts = usedPortsByContainer.get(containerKey);

    for (;; port++) {

      if (alreadySelected(containerKey, port, result)) {
        continue;
      }

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

  private boolean alreadySelected(String containerKey, int port, List<KeeperBasicInfo> result) {

    for (KeeperBasicInfo keeperSelected : result) {
      if (containerKeyOf(keeperSelected).equalsIgnoreCase(containerKey) && keeperSelected.getPort() == port) {
        return true;
      }
    }
    return false;
  }

  /**
   * 端口记账的键：物理机容器用 IP，pod 容器用 hostname。
   * <p>
   * 键不能换成 keepercontainerId —— 一台物理机上可以登记多个 keepercontainer 实例（同 IP
   * 不同管理端口），它们共享同一份 keeper 端口空间，按 id 分组会让同机实例间的端口冲突失去检测。
   * pod 则是一容器一 hostname，按 hostname 分组与按 IP 分组等价。
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

  private Map<String, Set<Integer>> getUsedPortsByContainerKey(List<KeepercontainerTbl> keeperCount) {

    Map<String, Set<Integer>> map = Maps.newHashMap();
    // 同一个键可能对应多行（同机多容器实例），查询只需按键做一次
    Map<String, KeepercontainerTbl> keyToContainer = Maps.newHashMap();
    keeperCount.forEach(kc -> {
      String key = containerKey(kc);
      map.putIfAbsent(key, new HashSet<Integer>());
      keyToContainer.putIfAbsent(key, kc);
    });

    List<Future> futures = new ArrayList<>(map.size());
    for(Map.Entry<String, Set<Integer>> entry : map.entrySet()) {

      String containerKey = entry.getKey();
      Set<Integer> existingPorts = entry.getValue();
      boolean pod = isPodContainer(keyToContainer.get(containerKey));
      Future future = executor.submit(new Runnable() {
        @Override
        public void run() {
          List<RedisTbl> redisOnSameContainer = pod
                  ? redisService.findAllRedisWithSameHostName(containerKey)
                  : redisService.findAllRedisWithSameIP(containerKey);
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
        return getUsedPortsByContainerKey(keeperCount);
      }
    }
    return map;
  }
}
