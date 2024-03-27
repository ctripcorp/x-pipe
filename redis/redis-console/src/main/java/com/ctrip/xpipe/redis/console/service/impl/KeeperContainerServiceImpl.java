package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.keeper.entity.KeeperContainerDiskType;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestOperations;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class KeeperContainerServiceImpl extends AbstractConsoleService<KeepercontainerTblDao>
    implements KeeperContainerService {

  @Autowired
  private ClusterService clusterService;

  @Autowired
  private DcService dcService;

  @Autowired
  private OrganizationService organizationService;

  @Autowired
  private RedisService redisService;

  @Autowired
  private AzService azService;

  private RestOperations restTemplate;

  @Override
  public KeepercontainerTbl find(final long id) {
    return queryHandler.handleQuery(new DalQuery<KeepercontainerTbl>() {
      @Override
      public KeepercontainerTbl doQuery() throws DalException {
        return dao.findByPK(id, KeepercontainerTblEntity.READSET_FULL);
      }
    });
  }

  @Override
  public KeepercontainerTbl find(final String ip) {
    return queryHandler.handleQuery(new DalQuery<KeepercontainerTbl>() {
      @Override
      public KeepercontainerTbl doQuery() throws DalException {
        return dao.findByIp(ip, KeepercontainerTblEntity.READSET_FULL);
      }
    });
  }

  @Override
  public List<KeepercontainerTbl> findAll() {
    return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
      @Override
      public List<KeepercontainerTbl> doQuery() throws DalException {
        return dao.findAll(KeepercontainerTblEntity.READSET_FULL);
      }
    });
  }

  @Override
  public List<KeepercontainerTbl> findAllByDcName(final String dcName) {
    return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
      @Override
      public List<KeepercontainerTbl> doQuery() throws DalException {
        return dao.findByDcName(dcName, KeepercontainerTblEntity.READSET_FULL);
      }
    });
  }

  @Override
  public List<KeepercontainerTbl> findAllActiveByDcName(String dcName) {
    return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
      @Override
      public List<KeepercontainerTbl> doQuery() throws DalException {
        return dao.findActiveByDcName(dcName, KeepercontainerTblEntity.READSET_FULL);
      }
    });
  }

  @Override
  public List<KeepercontainerTbl> findKeeperCount(String dcName) {
    return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
      @Override
      public List<KeepercontainerTbl> doQuery() throws DalException {
        return dao.findKeeperCount(dcName, KeepercontainerTblEntity.READSET_KEEPER_COUNT);
      }
    });
  }

  @Override
  public List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName) {
    /*
     * 1. BU has its own keepercontainer(kc), then find all and see if it satisfied the requirement
     * 2. Cluster don't have a BU, find default one
     * 3. BU don't have its own kc, find in the normal kc pool(org id is 0L)
     */
    long clusterOrgId;
    if (clusterName != null) {
      ClusterTbl clusterTbl = clusterService.find(clusterName);
      clusterOrgId = clusterTbl == null ? XPipeConsoleConstant.DEFAULT_ORG_ID : clusterTbl.getClusterOrgId();
    } else {
      clusterOrgId = XPipeConsoleConstant.DEFAULT_ORG_ID;
    }
    logger.info("cluster org id: {}", clusterOrgId);
    return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
      @Override
      public List<KeepercontainerTbl> doQuery() throws DalException {
        // find all keepers, both in used and unused
        List<KeepercontainerTbl> allDcKeeperContainers = dao.findActiveByDcName(dcName, KeepercontainerTblEntity.READSET_FULL);
        List<KeepercontainerTbl> allDcOrgKeeperContainers = allDcKeeperContainers.stream().filter(keepercontainer -> keepercontainer.getKeepercontainerOrgId() == clusterOrgId).collect(Collectors.toList());

        List<KeepercontainerTbl> dcOrgKeeperContainersInUsed;
        if (allDcOrgKeeperContainers.isEmpty() && clusterOrgId != XPipeConsoleConstant.DEFAULT_ORG_ID) {
          logger.info("cluster {} with org id {} is going to find keepercontainers in normal pool",
                  clusterName, clusterOrgId);
          allDcOrgKeeperContainers = allDcKeeperContainers.stream().filter(keepercontainer -> keepercontainer.getKeepercontainerOrgId() == XPipeConsoleConstant.DEFAULT_ORG_ID).collect(Collectors.toList());

          // find keepers in used in normal org
          dcOrgKeeperContainersInUsed = dao.findKeeperContainerByCluster(dcName, XPipeConsoleConstant.DEFAULT_ORG_ID,
                  KeepercontainerTblEntity.READSET_KEEPER_COUNT_BY_CLUSTER);
        } else {
          // find keepers in used in cluster org
          dcOrgKeeperContainersInUsed = dao.findKeeperContainerByCluster(dcName, clusterOrgId,
                  KeepercontainerTblEntity.READSET_KEEPER_COUNT_BY_CLUSTER);
        }

        setCountAndSortForAllKeeperContainers(allDcOrgKeeperContainers,  dcOrgKeeperContainersInUsed);
        allDcOrgKeeperContainers = filterKeeperFromSameAvailableZone(allDcOrgKeeperContainers, dcName);
        logger.info("find keeper containers: {}", allDcOrgKeeperContainers);
        return allDcOrgKeeperContainers;
      }
    });
  }

  void setCountAndSortForAllKeeperContainers(List<KeepercontainerTbl> allKeepers, List<KeepercontainerTbl> keepersInUsed) {
    Map<Long, KeepercontainerTbl> dcKeepersInOrgMap = allKeepers.stream().collect(Collectors.toMap(KeepercontainerTbl::getKeepercontainerId, dcKeeperContainerInOrg -> dcKeeperContainerInOrg));
    Map<Long, KeepercontainerTbl> dcKeepersInOrgWithCountMap = keepersInUsed.stream().collect(Collectors.toMap(KeepercontainerTbl::getKeepercontainerId, dcKeeperContainerInOrgWithCount -> dcKeeperContainerInOrgWithCount));

    for (long keeperId : dcKeepersInOrgWithCountMap.keySet()) {
      KeepercontainerTbl existed = dcKeepersInOrgMap.get(keeperId);
      if (existed != null) {
        existed.setCount(dcKeepersInOrgWithCountMap.get(keeperId).getCount());
      }
    }
    allKeepers.sort(Comparator.comparing(KeepercontainerTbl::getCount));
  }

  @Override
  public List<KeepercontainerTbl> getKeeperContainerByAz(Long azId) {
    return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
      @Override
      public List<KeepercontainerTbl> doQuery() throws DalException {
        return dao.findByAzId(azId, KeepercontainerTblEntity.READSET_FULL);
      }
    });
  }

  private List<KeepercontainerTbl>  filterKeeperFromSameAvailableZone(List<KeepercontainerTbl> keepercontainerTbls, String dcName) {
    List<AzTbl> dcAvailableZones = azService.getDcActiveAvailableZoneTbls(dcName);
    if(dcAvailableZones == null || dcAvailableZones.isEmpty() || dcAvailableZones.size() == 1) {
      return keepercontainerTbls;
    } else {
      Set<Long> usedAvailableZones = new HashSet<>();
      Map<Long, AzTbl> availableZoneMap = new HashMap();
      dcAvailableZones.forEach((availableZone)-> {
        availableZoneMap.put(availableZone.getId(), availableZone);
      });

      List<KeepercontainerTbl> result = new ArrayList<>();
      for (KeepercontainerTbl keepercontainerTbl : keepercontainerTbls) {
        long azId = keepercontainerTbl.getAzId();
        if (!availableZoneMap.containsKey(azId))
          throw new XpipeRuntimeException(String.format("This keepercontainer %s:%d has unknown available zone id %d "
                  ,keepercontainerTbl.getKeepercontainerIp(), keepercontainerTbl.getKeepercontainerPort(), azId));

        if (availableZoneMap.get(azId).isActive() && usedAvailableZones.add(azId)) {
          result.add(keepercontainerTbl);
        }
      }
      return result;
    }
  }

  protected void update(KeepercontainerTbl keepercontainerTbl) {

    queryHandler.handleUpdate(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.updateByPK(keepercontainerTbl, KeepercontainerTblEntity.UPDATESET_FULL);
      }
    });
  }

  @Override
  public void addKeeperContainer(final KeeperContainerCreateInfo createInfo) {

    KeepercontainerTbl proto = dao.createLocal();

    if(keeperContainerAlreadyExists(createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort())) {
      throw new IllegalArgumentException("Keeper Container with IP: "
              + createInfo.getKeepercontainerIp() + " already exists");
    }

    if (!checkIpAndPort(createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort())) {
      throw new IllegalArgumentException(String.format("Keeper container with ip:%s, port:%d is unhealthy",
              createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort()));
    }

    DcTbl dcTbl = dcService.find(createInfo.getDcName());
    if(dcTbl == null) {
      throw new IllegalArgumentException("DC name does not exist");
    }

    OrganizationTbl org;
    if(createInfo.getKeepercontainerOrgId() == 0) {
      org = new OrganizationTbl().setId(0L);
    } else {
      org = organizationService.getOrganizationTblByCMSOrganiztionId(createInfo.getKeepercontainerOrgId());
      if (org == null) {
        throw new IllegalArgumentException("Org Id does not exist in database");
      }
    }

    if (createInfo.getAzName() != null) {
      AzTbl aztbl = azService.getAvailableZoneTblByAzName(createInfo.getAzName());
      if(aztbl == null) {
        throw new IllegalArgumentException(String.format("available zone %s is not exist", createInfo.getAzName()));
      }
      proto.setAzId(aztbl.getId());
    }

    proto.setKeepercontainerDc(dcTbl.getId())
            .setKeepercontainerIp(createInfo.getKeepercontainerIp())
            .setKeepercontainerPort(createInfo.getKeepercontainerPort())
            .setKeepercontainerOrgId(org.getId())
            .setKeepercontainerActive(createInfo.isActive())
            .setKeepercontainerDiskType(createInfo.getDiskType() == null ? KeeperContainerDiskType.DEFAULT.getDesc() : createInfo.getDiskType().toUpperCase());

    queryHandler.handleInsert(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.insert(proto);
      }
    });
  }

  @Override
  public List<KeeperContainerCreateInfo> getDcAllKeeperContainers(String dc) {
    List<KeepercontainerTbl> keepercontainerTbls = queryHandler.handleQuery(() ->
            dao.findByDcName(dc, KeepercontainerTblEntity.READSET_FULL));

    OrgInfoTranslator translator = new OrgInfoTranslator();
    return Lists.newArrayList(Lists.transform(keepercontainerTbls, new Function<KeepercontainerTbl, KeeperContainerCreateInfo>() {
      @Override
      public KeeperContainerCreateInfo apply(KeepercontainerTbl input) {
        OrganizationTbl org = translator.getFromXPipeId(input.getKeepercontainerOrgId());

        KeeperContainerCreateInfo info = new KeeperContainerCreateInfo()
                .setDcName(dc).setActive(input.isKeepercontainerActive())
                .setKeepercontainerIp(input.getKeepercontainerIp())
                .setKeepercontainerPort(input.getKeepercontainerPort())
                .setDiskType(input.getKeepercontainerDiskType());
        if (org != null) {
          info.setKeepercontainerOrgId(org.getOrgId()).setOrgName(org.getOrgName());
        } else {
          info.setKeepercontainerOrgId(0L);
        }

        if (input.getAzId() != 0) {
          AzTbl aztbl = azService.getAvailableZoneTblById(input.getAzId());
          if(aztbl == null) {
            throw new XpipeRuntimeException(String.format("dc %s do not has available zone %d", dc, input.getAzId()));
          }
          info.setAzName(aztbl.getAzName());
        }
        return info;
      }
    }));
  }

  @Override
  public void
  updateKeeperContainer(KeeperContainerCreateInfo createInfo) {
    KeepercontainerTbl keepercontainerTbl = findByIpPort(createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort());
    if(keepercontainerTbl == null) {
      throw new IllegalArgumentException(String.format("%s:%d keeper container not found",
              createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort()));
    }
    if(createInfo.getKeepercontainerOrgId() != 0L) {
      OrganizationTbl org = organizationService.getOrganizationTblByCMSOrganiztionId(createInfo.getKeepercontainerOrgId());
      keepercontainerTbl.setKeepercontainerOrgId(org.getId());
    } else {
      keepercontainerTbl.setKeepercontainerOrgId(0L);
    }

    if (createInfo.getAzName() != null) {
      AzTbl aztbl = azService.getAvailableZoneTblByAzName(createInfo.getAzName());
      if(aztbl == null) {
        throw new IllegalArgumentException(String.format("available zone %s is not exist", createInfo.getAzName()));
      }
      keepercontainerTbl.setAzId(aztbl.getId());
    }

    keepercontainerTbl.setKeepercontainerActive(createInfo.isActive());

    keepercontainerTbl.setKeepercontainerDiskType(createInfo.getDiskType());
    queryHandler.handleUpdate(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.updateByPK(keepercontainerTbl, KeepercontainerTblEntity.UPDATESET_FULL);
      }
    });
  }

  @Override
  public void deleteKeeperContainer(String keepercontainerIp, int keepercontainerPort) {
    KeepercontainerTbl keepercontainerTbl = findByIpPort(keepercontainerIp, keepercontainerPort);
    if(null == keepercontainerTbl) throw new BadRequestException("Cannot find keepercontainer");

    List<RedisTbl> keepers = redisService.findAllRedisWithSameIP(keepercontainerIp);
    if(keepers != null && !keepers.isEmpty()) {
      throw new BadRequestException(String.format("This keepercontainer %s:%d is not empty, unable to delete!", keepercontainerIp, keepercontainerPort));
    }

    KeepercontainerTbl proto = keepercontainerTbl;
    queryHandler.handleDelete(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.deleteKeeperContainer(proto, KeepercontainerTblEntity.UPDATESET_FULL);
      }
    }, true);
  }

  @Override
  public void addKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel) {
    KeepercontainerTbl proto = dao.createLocal();

    if(keeperContainerAlreadyExists(keeperContainerInfoModel.getAddr().getHost(), keeperContainerInfoModel.getAddr().getPort())) {
      throw new IllegalArgumentException("Keeper Container with IP: "
                                                  + keeperContainerInfoModel.getAddr().getHost() + " already exists");
    }

    if (!checkIpAndPort(keeperContainerInfoModel.getAddr().getHost(), keeperContainerInfoModel.getAddr().getPort())) {
      throw new IllegalArgumentException(String.format("Keeper container with ip:%s, port:%d is unhealthy",
              keeperContainerInfoModel.getAddr().getHost(), keeperContainerInfoModel.getAddr().getPort()));
    }

    DcTbl dcTbl = dcService.find(keeperContainerInfoModel.getDcName());
    if(dcTbl == null) {
      throw new IllegalArgumentException("DC name does not exist");
    }

    OrganizationTbl org;
    if (keeperContainerInfoModel.getOrgName() == null
            || (org = organizationService.getOrgByName(keeperContainerInfoModel.getOrgName())) == null) {
      proto.setKeepercontainerOrgId(0L);
    } else {
      proto.setKeepercontainerOrgId(org.getId());
    }

    if (keeperContainerInfoModel.getAzName() != null) {
      AzTbl azTbl = azService.getAvailableZoneTblByAzName(keeperContainerInfoModel.getAzName());
      if(azTbl == null) {
        throw new IllegalArgumentException(String.format("available zone %s does not exist",
                                                                      keeperContainerInfoModel.getAzName()));
      }
      proto.setAzId(azTbl.getId());
    }

    proto.setKeepercontainerDc(dcTbl.getId())
            .setKeepercontainerIp(keeperContainerInfoModel.getAddr().getHost())
            .setKeepercontainerPort(keeperContainerInfoModel.getAddr().getPort())
            .setKeepercontainerActive(keeperContainerInfoModel.isActive());

    queryHandler.handleInsert(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.insert(proto);
      }
    });
  }

  @Override
  public void updateKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel) {
    KeepercontainerTbl proto =
            findByIpPort(keeperContainerInfoModel.getAddr().getHost(), keeperContainerInfoModel.getAddr().getPort());
    if( proto == null) {
      throw new IllegalArgumentException("Keeper container with IP: " + keeperContainerInfoModel.getAddr().getHost()
              + " does not exists");
    }

    OrganizationTbl org;
    if (keeperContainerInfoModel.getOrgName() == null
            || (org = organizationService.getOrgByName(keeperContainerInfoModel.getOrgName())) == null) {
      proto.setKeepercontainerOrgId(0L);
    } else {
      proto.setKeepercontainerOrgId(org.getId());
    }

    if (!StringUtil.isEmpty(keeperContainerInfoModel.getAzName())) {
      AzTbl azTbl = azService.getAvailableZoneTblByAzName(keeperContainerInfoModel.getAzName());
      if(azTbl == null) {
        throw new IllegalArgumentException(String.format("available zone %s does not exist",
                                                                                keeperContainerInfoModel.getAzName()));
      }
      proto.setAzId(azTbl.getId());
    } else {
      proto.setAzId(0L);
    }

    proto.setKeepercontainerActive(keeperContainerInfoModel.isActive());

    proto.setKeepercontainerDiskType(keeperContainerInfoModel.getDiskType());

    queryHandler.handleQuery(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.updateByPK(proto, KeepercontainerTblEntity.UPDATESET_FULL);
      }
    });
  }

  @Override
  public List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp) {
    getOrCreateRestTemplate();
    return restTemplate.exchange(String.format("http://%s:8080/keepers", keeperContainerIp), HttpMethod.GET, null,
            new ParameterizedTypeReference<List<KeeperInstanceMeta>>() {}).getBody();
  }

  @Override
  public void resetKeepers(KeeperTransMeta keeperInstanceMeta) {
    getOrCreateRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity<KeeperTransMeta> requestEntity = new HttpEntity<>(keeperInstanceMeta, headers);
    restTemplate.exchange(String.format("http://%s:8080/keepers/election/reset", keeperInstanceMeta.getKeeperMeta().getIp()),
            HttpMethod.POST, requestEntity, Void.class);
  }

  @Override
  public Map<Long, Long> keeperContainerIdDcMap() {
    Map<Long, Long> keeperContainerIdDcMap = new HashMap<>();
    List<KeepercontainerTbl> allKeeperContainers = findAll();
    allKeeperContainers.forEach((keeperContainer) -> {
      keeperContainerIdDcMap.put(keeperContainer.getKeyKeepercontainerId(), keeperContainer.getKeepercontainerDc());
    });
    return keeperContainerIdDcMap;
  }

  @Override
  public List<Set<Long>> divideKeeperContainers(int partsCount) {
    List<KeepercontainerTbl> all = findAll();
    if (all == null) return Collections.emptyList();

    List<Set<Long>> result = new ArrayList<>(partsCount);
    IntStream.range(0, partsCount).forEach(i -> result.add(new HashSet<>()));

    all.forEach(keeperContainer -> result.get((int) keeperContainer.getKeepercontainerId() % partsCount)
            .add(keeperContainer.getKeepercontainerId()));

    return result;
  }


  @Override
  public List<KeeperContainerInfoModel> findAllInfos() {
    List<KeepercontainerTbl> baseInfos = findContainerBaseInfos();

    HashMap<Long, KeeperContainerInfoModel> containerInfoMap = new HashMap<>();
    baseInfos.forEach(baseInfo -> {
      KeeperContainerInfoModel model = new KeeperContainerInfoModel();
      model.setId(baseInfo.getKeepercontainerId());
      model.setActive(baseInfo.isKeepercontainerActive());
      model.setAddr(new HostPort(baseInfo.getKeepercontainerIp(), baseInfo.getKeepercontainerPort()));
      model.setDcName(baseInfo.getDcInfo().getDcName());
      model.setOrgName(baseInfo.getOrgInfo().getOrgName());
      model.setDiskType(baseInfo.getKeepercontainerDiskType());

      if (baseInfo.getAzId() != 0) {
        AzTbl aztbl = azService.getAvailableZoneTblById(baseInfo.getAzId());
        if(aztbl == null) {
          throw new XpipeRuntimeException(String.format("dc %s do not has available zone %d", baseInfo.getDcInfo().getDcName(), baseInfo.getAzId()));
        }
        model.setAzName(aztbl.getAzName());
      }

      containerInfoMap.put(model.getId(), model);
    });

    List<RedisTbl> containerLoad = redisService.findAllKeeperContainerCountInfo();
    containerLoad.forEach(load -> {
      if (!containerInfoMap.containsKey(load.getKeepercontainerId())) return;
      KeeperContainerInfoModel model = containerInfoMap.get(load.getKeepercontainerId());
      model.setKeeperCount(load.getCount());
      model.setClusterCount(load.getDcClusterShardInfo().getClusterCount());
      model.setShardCount(load.getDcClusterShardInfo().getShardCount());
    });

    return new ArrayList<>(containerInfoMap.values());
  }

  @Override
  public KeeperContainerInfoModel findKeeperContainerInfoModelById(long id) {
    Map<Long, String> dcNameMap = dcService.dcNameMap();
    Map<Long, String> azNameMap = azService.azNameMap();

    KeepercontainerTbl keepercontainerTbl = find(id);
    KeeperContainerInfoModel keeperContainerInfoModel = new KeeperContainerInfoModel();
    keeperContainerInfoModel.setId(keepercontainerTbl.getKeepercontainerId());
    keeperContainerInfoModel.setActive(keepercontainerTbl.isKeepercontainerActive());
    keeperContainerInfoModel.setDcName(dcNameMap.get(keepercontainerTbl.getKeepercontainerDc()));
    keeperContainerInfoModel.setAzName(azNameMap.get(keepercontainerTbl.getAzId()));
    keeperContainerInfoModel.setAddr(new HostPort(keepercontainerTbl.getKeepercontainerIp(), keepercontainerTbl.getKeepercontainerPort()));
    keeperContainerInfoModel.setDiskType(keepercontainerTbl.getKeepercontainerDiskType());

    OrganizationTbl organizationTbl = organizationService.getOrganization(keepercontainerTbl.getKeepercontainerOrgId());
    if (organizationTbl != null) {
      keeperContainerInfoModel.setOrgName(organizationTbl.getOrgName());
    }

    return keeperContainerInfoModel;
  }

  @Override
  public List<KeeperContainerInfoModel> findAvailableKeeperContainerInfoModelsByDcAzAndOrg(String dcName, String azName, String orgName) {
    long azId = 0;
    long orgId = 0;

    if (!StringUtil.isEmpty(azName)) {
      AzTbl azTbl = azService.getAvailableZoneTblByAzName(azName);
      azId = azTbl == null ? 0 : azTbl.getId();
    }
    if (!StringUtil.isEmpty(orgName)) {
      OrganizationTbl org = organizationService.getOrgByName(orgName);
      orgId = org == null ? 0 : org.getId();
    }

    List<KeeperContainerInfoModel> result = new ArrayList<>();

    List<KeepercontainerTbl> activeKeepercontainers = findAllActiveByDcName(dcName);
    for (KeepercontainerTbl keepercontainer : activeKeepercontainers) {
      if (azId != 0 && azId != keepercontainer.getAzId()) {
        continue;
      }
      if (orgId != keepercontainer.getKeepercontainerOrgId()) {
        continue;
      }

      KeeperContainerInfoModel keeperContainerInfoModel = new KeeperContainerInfoModel();
      keeperContainerInfoModel.setId(keepercontainer.getKeepercontainerId());
      keeperContainerInfoModel.setActive(keepercontainer.isKeepercontainerActive());
      keeperContainerInfoModel.setDcName(dcName);
      keeperContainerInfoModel.setAzName(azName);
      keeperContainerInfoModel.setAddr(new HostPort(keepercontainer.getKeepercontainerIp(), keepercontainer.getKeepercontainerPort()));
      keeperContainerInfoModel.setOrgName(orgName);
      result.add(keeperContainerInfoModel);
     }

    return result;
  }

  private List<KeepercontainerTbl> findContainerBaseInfos() {
    return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
      @Override
      public List<KeepercontainerTbl> doQuery() throws DalException {
        return dao.findContainerBaseInfo(KeepercontainerTblEntity.READSET_BASE_INFO);
      }
    });
  }

  protected KeepercontainerTbl findByIpPort(String ip, int port) {
    return queryHandler.handleQuery(new DalQuery<KeepercontainerTbl>() {
      @Override
      public KeepercontainerTbl doQuery() throws DalException {
        return dao.findByIpPort(ip, port, KeepercontainerTblEntity.READSET_FULL);
      }
    });
  }

  protected boolean keeperContainerAlreadyExists(String ip, int port) {
    KeepercontainerTbl existing = queryHandler.handleQuery(new DalQuery<KeepercontainerTbl>() {
      @Override
      public KeepercontainerTbl doQuery() throws DalException {
        return dao.findByIpPort(ip, port,
                KeepercontainerTblEntity.READSET_CONTAINER_ADDRESS);
      }
    });
    return existing != null;
  }

  protected void getOrCreateRestTemplate() {
    if (restTemplate == null) {
      synchronized (this) {
        if (restTemplate == null) {
          restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(10, 20, 3000, 5000);
        }
      }
    }
  }

  @VisibleForTesting
  protected void setRestTemplate(RestOperations restTemplate) {
    this.restTemplate = restTemplate;
  }

  protected boolean checkIpAndPort(String host, int port) {

    getOrCreateRestTemplate();
    String url = "http://%s:%d/health";
    try {
      Boolean result = restTemplate.getForObject(String.format(url, host, port), Boolean.class);
      if (result == null) {
          throw new XpipeRuntimeException("result of checkIpAndPort is null");
      }
      return result;
    } catch (RestClientException e) {
      logger.error("[healthCheck]Http connect occur exception. ", e);
    }
    return false;
  }

  private class OrgInfoTranslator {

    private Map<Long, OrganizationTbl> cache = Maps.newHashMap();

    private OrganizationTbl getFromXPipeId(long id) {
      if(id == 0L) {
        return null;
      }
      if(cache.containsKey(id)) {
        return cache.get(id);
      }
      OrganizationTbl org = organizationService.getOrganization(id);
      cache.put(id, org);
      return org;
    }
  }
}
