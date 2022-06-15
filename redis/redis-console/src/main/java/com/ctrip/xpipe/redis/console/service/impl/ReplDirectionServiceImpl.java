package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ReplDirectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ReplDirectionServiceImpl  extends AbstractConsoleService<ReplDirectionTblDao>
        implements ReplDirectionService {

    @Autowired
    ClusterService clusterService;

    @Autowired
    DcService dcService;

    @Override
    public ReplDirectionTbl find(long id) {
        return queryHandler.handleQuery(new DalQuery<ReplDirectionTbl>() {
            @Override
            public ReplDirectionTbl doQuery() throws DalException {
                return dao.findByPK(id, ReplDirectionTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ReplDirectionTbl> findAllReplDirectionByCluster(long clusterId) {

        return queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findReplDirectionByCluster(clusterId, ReplDirectionTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ReplDirectionInfoModel> findReplDirectionInfoModelByClusterAndToDc(String clusterName, String toDcName) {
        ClusterTbl cluster = clusterService.find(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(String.format("cluster %s does not exist", clusterName));
        }

        DcTbl dc = dcService.find(toDcName);
        if (dc == null) {
            throw new IllegalArgumentException(String.format("dc %s does not exist", toDcName));
        }

        List<ReplDirectionTbl> replDirectionTbls = queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findReplDirectionByClusterAndToDc(cluster.getId(), dc.getId(), ReplDirectionTblEntity.READSET_FULL);
            }
        });

        Map<Long, String> dcNameMap = dcService.dcNameMap();
        List<ReplDirectionInfoModel> result = new ArrayList<>();
        for (ReplDirectionTbl replDirectionTbl : replDirectionTbls) {
            result.add(convertReplDirectionTblToReplDirectionInfoModel(replDirectionTbl, dcNameMap));
        }

        return result;
    }

    private ReplDirectionInfoModel convertReplDirectionTblToReplDirectionInfoModel(ReplDirectionTbl replDirectionTbl,
                                                                                   Map<Long, String> dcNameMap) {
        ReplDirectionInfoModel replDirectionInfoModel = new ReplDirectionInfoModel();
        replDirectionInfoModel.setId(replDirectionTbl.getId())
                .setClusterName(clusterService.find(replDirectionTbl.getClusterId()).getClusterName())
                .setSrcDcName(dcNameMap.get(replDirectionTbl.getSrcDcId()))
                .setFromDcName(dcNameMap.get(replDirectionTbl.getFromDcId()))
                .setToDcName(dcNameMap.get(replDirectionTbl.getToDcId()));

        ClusterTbl clusterTbl = clusterService.find(replDirectionTbl.getClusterId());
        if (null == clusterTbl)
            throw new IllegalArgumentException(String.format("cluster %d does not exist", replDirectionTbl.getClusterId()));

        replDirectionInfoModel.setClusterName(clusterTbl.getClusterName());
        return replDirectionInfoModel;
    }


    @Override
    public void updateReplDirection(ReplDirectionInfoModel model) {
        ReplDirectionTbl proto = dao.createLocal();

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(proto, ReplDirectionTblEntity.UPDATESET_FULL);
            }
        });
    }

}
