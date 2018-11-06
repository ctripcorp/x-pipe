package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Map;

@Service
public class KeepercontainerServiceImpl extends AbstractConsoleService<KeepercontainerTblDao>
    implements KeepercontainerService {

  @Autowired
  private ClusterService clusterService;

  @Autowired
  private DcService dcService;

  @Autowired
  private OrganizationService organizationService;

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
        logger.info("find keeper containers: {}", kcs);
        return kcs;
      }
    });
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
    KeepercontainerTbl kc = findByIpPort(createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort());
    if(kc == null) {
      throw new IllegalArgumentException(String.format("%s:%d keeper container not found",
              createInfo.getKeepercontainerIp(), createInfo.getKeepercontainerPort()));
    }
    if(createInfo.getKeepercontainerOrgId() != 0L) {
      OrganizationTbl org = organizationService.getOrganizationTblByCMSOrganiztionId(createInfo.getKeepercontainerOrgId());
      kc.setKeepercontainerOrgId(org.getId());
    } else {
      kc.setKeepercontainerOrgId(0L);
    }
    kc.setKeepercontainerActive(createInfo.isActive());
    queryHandler.handleUpdate(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.updateByPK(kc, KeepercontainerTblEntity.UPDATESET_FULL);
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

  private boolean keeperContainerAlreadyExists(KeeperContainerCreateInfo createInfo) {
    List<KeepercontainerTbl> keepercontainerTbls = findAllByDcName(createInfo.getDcName());
    for(KeepercontainerTbl kc : keepercontainerTbls) {
      if(StringUtil.trimEquals(kc.getKeepercontainerIp(), createInfo.getKeepercontainerIp())) {
        return true;
      }
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
