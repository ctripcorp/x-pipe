package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestOperations;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        List<KeepercontainerTbl> kcs = dao.findKeeperContainerByCluster(dcName, clusterOrgId,
            KeepercontainerTblEntity.READSET_KEEPER_COUNT_BY_CLUSTER);
        if (kcs == null || kcs.isEmpty()) {
          logger.info("cluster {} with org id {} is going to find keepercontainers in normal pool",
                  clusterName, clusterOrgId);
          kcs = dao.findKeeperContainerByCluster(dcName, XPipeConsoleConstant.DEFAULT_ORG_ID,
              KeepercontainerTblEntity.READSET_KEEPER_COUNT_BY_CLUSTER);
        }
        kcs = filterKeeperFromSameAvailableZone(kcs, dcName);
        logger.info("find keeper containers: {}", kcs);
        return kcs;
      }
    });
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

  private List<KeepercontainerTbl>  filterKeeperFromSameAvailableZone(List<KeepercontainerTbl> kcs, String dcName) {
    List<AzTbl> dcAvailableZones = azService.getDcActiveAvailableZoneTbls(dcName);
    if(dcAvailableZones == null || dcAvailableZones.isEmpty()) {
      return kcs;
    } else {
      Map<Long, Boolean> azs = new HashMap();
      dcAvailableZones.forEach((az)-> {
        azs.put(az.getId(), false);
      });

      List<KeepercontainerTbl> result = new ArrayList<>();
      for (KeepercontainerTbl kc : kcs) {
        long azId = kc.getAzId();
        if(azs.get(azId) != null && !azs.get(azId)) {
          azs.put(azId, true);
          result.add(kc);
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

    if(keeperContainerAlreadyExists(createInfo)) {
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

    proto.setKeepercontainerDc(dcTbl.getId())
            .setKeepercontainerIp(createInfo.getKeepercontainerIp())
            .setKeepercontainerPort(createInfo.getKeepercontainerPort())
            .setKeepercontainerOrgId(org.getId())
            .setKeepercontainerActive(createInfo.isActive());

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
                .setKeepercontainerPort(input.getKeepercontainerPort());
        if(org != null) {
          info.setKeepercontainerOrgId(org.getOrgId()).setOrgName(org.getOrgName());
        } else {
          info.setKeepercontainerOrgId(0L);
        }
        return info;
      }
    }));
  }

  @Override
  public void updateKeeperContainer(KeeperContainerCreateInfo createInfo) {
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
    keepercontainerTbl.setKeepercontainerActive(createInfo.isActive());
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
  public void deleteKeeperContainers(List<KeepercontainerTbl> keepercontainerTbls) {
    for (KeepercontainerTbl keepercontainerTbl : keepercontainerTbls) {
      List<RedisTbl> keepers = redisService.findAllRedisWithSameIP(keepercontainerTbl.getKeepercontainerIp());
      if(keepers != null && !keepers.isEmpty()) {
        throw new BadRequestException(String.format("This keepercontainer %s:%d is not empty, unable to delete!", keepercontainerTbl.getKeepercontainerIp(), keepercontainerTbl.getKeepercontainerPort()));
      }
    }

    for (KeepercontainerTbl keepercontainerTbl : keepercontainerTbls) {
      KeepercontainerTbl proto = keepercontainerTbl;
      queryHandler.handleDelete(new DalQuery<Integer>() {
        @Override
        public Integer doQuery() throws DalException {
          return dao.deleteKeeperContainer(proto, KeepercontainerTblEntity.UPDATESET_FULL);
        }
      }, true);
    }
  }

  @Override
  public List<KeeperContainerInfoModel> findAllInfos() {
    List<KeepercontainerTbl> baseInfos = findContainerBaseInfos();

    HashMap<Long, KeeperContainerInfoModel> containerInfoMap = new HashMap<>();
    baseInfos.forEach(baseInfo -> {
      KeeperContainerInfoModel model = new KeeperContainerInfoModel();
      model.setId(baseInfo.getKeepercontainerId());
      model.setAddr(new HostPort(baseInfo.getKeepercontainerIp(), baseInfo.getKeepercontainerPort()));
      model.setDcName(baseInfo.getDcInfo().getDcName());
      model.setOrgName(baseInfo.getOrgInfo().getOrgName());

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

  protected boolean keeperContainerAlreadyExists(KeeperContainerCreateInfo createInfo) {
    KeepercontainerTbl existing = queryHandler.handleQuery(new DalQuery<KeepercontainerTbl>() {
      @Override
      public KeepercontainerTbl doQuery() throws DalException {
        return dao.findByIpPort(createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort(),
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
      return restTemplate.getForObject(String.format(url, host, port), Boolean.class);

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
