package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ApplierDao;
import com.ctrip.xpipe.redis.console.model.ApplierTbl;
import com.ctrip.xpipe.redis.console.model.ApplierTblDao;
import com.ctrip.xpipe.redis.console.model.ApplierTblEntity;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.ClusterMonitorModifiedNotifier;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;

@Service
public class ApplierServiceImpl extends AbstractConsoleService<ApplierTblDao> implements ApplierService {

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private AppliercontainerService appliercontainerService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ClusterMetaModifiedNotifier notifier;

    @Autowired
    private DcService dcService;

    @Autowired
    private ApplierDao applierDao;

    @Autowired
    private ClusterMonitorModifiedNotifier monitorNotifier;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    ExecutorService executor;


    @Override
    public ApplierTbl find(long id) {
        return queryHandler.handleQuery(new DalQuery<ApplierTbl>() {
            @Override
            public ApplierTbl doQuery() throws DalException {
                return dao.findByPK(id, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public ApplierTbl findByIpPort(String ip, int port) {
        return queryHandler.handleQuery(new DalQuery<ApplierTbl>() {
            @Override
            public ApplierTbl doQuery() throws DalException {
                return dao.findByIpPort(ip, port, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ApplierTbl> findByShardAndReplDirection(long shardId, long replDirectionId) {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return dao.findAllByShardAndReplDirection(shardId, replDirectionId, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ApplierTbl> findAllAppliersWithSameIp(String ip) {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return dao.findByIp(ip, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    public List<ApplierTbl> findAllAppliercontainerCountInfo() {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return dao.countContainerApplierAndClusterAndShard(ApplierTblEntity.READSET_CONTAINER_LOAD);
            }
        });
    }
}
