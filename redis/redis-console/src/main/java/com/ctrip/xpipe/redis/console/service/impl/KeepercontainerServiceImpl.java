package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

@Service
public class KeepercontainerServiceImpl extends AbstractConsoleService<KeepercontainerTblDao>
    implements KeepercontainerService {

  @Autowired
  private ClusterService clusterService;

  @Autowired
  private DcService dcService;

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
  public void addKeeperContainer(final KeepercontainerTbl keepercontainerTbl) {

    KeepercontainerTbl proto = dao.createLocal();

    if(!checkRequiredFields(keepercontainerTbl)) {
      throw new IllegalArgumentException("Argument missing, keeper container dc_id, ip, port and org_id is needed");
    }

    if(keeperContainerAlreadyExists(keepercontainerTbl)) {
      throw new IllegalArgumentException("Keeper Container with IP: "
              + keepercontainerTbl.getKeepercontainerIp() + " already exists");
    }

    proto.setKeepercontainerDc(keepercontainerTbl.getKeepercontainerDc())
            .setKeepercontainerIp(keepercontainerTbl.getKeepercontainerIp())
            .setKeepercontainerPort(keepercontainerTbl.getKeepercontainerPort())
            .setKeepercontainerOrgId(keepercontainerTbl.getOrgId())
            .setKeepercontainerActive(true);

    queryHandler.handleInsert(new DalQuery<Integer>() {
      @Override
      public Integer doQuery() throws DalException {
        return dao.insert(proto);
      }
    });
  }

  private boolean checkRequiredFields(KeepercontainerTbl keepercontainerTbl) {

    if(StringUtil.isEmpty(keepercontainerTbl.getKeepercontainerIp()))
      return false;
    if(keepercontainerTbl.getKeepercontainerPort() == 0)
      return false;

    return true;
  }

  private boolean keeperContainerAlreadyExists(KeepercontainerTbl keepercontainerTbl) {
    DcTbl dcTbl = dcService.find(keepercontainerTbl.getKeepercontainerDc());
    List<KeepercontainerTbl> keepercontainerTbls = findAllByDcName(dcTbl.getDcName());
    for(KeepercontainerTbl kc : keepercontainerTbls) {
      if(StringUtil.trimEquals(kc.getKeepercontainerIp(), keepercontainerTbl.getKeepercontainerIp())) {
        return true;
      }
    }
    return false;
  }
}
