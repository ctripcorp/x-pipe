package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.utils.StringUtil;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(false)
public class LogicalBuServiceImpl extends AbstractConsoleService<LogicalBuTblDao> implements LogicalBuService {

    private LogicalBuOrgTblDao logicalBuOrgTblDao;

    private KeepercontainerTblDao keepercontainerTblDao;

    @PostConstruct
    private void initAdditionalDaos() {
        try {
            logicalBuOrgTblDao = ContainerLoader.getDefaultContainer().lookup(LogicalBuOrgTblDao.class);
            keepercontainerTblDao = ContainerLoader.getDefaultContainer().lookup(KeepercontainerTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    @Override
    public List<LogicalBuModel> findAll() {
        List<LogicalBuTbl> all = queryHandler.handleQuery(new DalQuery<List<LogicalBuTbl>>() {
            @Override
            public List<LogicalBuTbl> doQuery() throws DalException {
                return dao.findAll(LogicalBuTblEntity.READSET_FULL);
            }
        });
        if (all == null) {
            return Collections.emptyList();
        }
        return all.stream().map(this::toModel).collect(Collectors.toList());
    }

    @Override
    public LogicalBuModel findById(long id) {
        LogicalBuTbl logicalBuTbl = queryHandler.handleQuery(new DalQuery<LogicalBuTbl>() {
            @Override
            public LogicalBuTbl doQuery() throws DalException {
                return dao.findById(id, LogicalBuTblEntity.READSET_FULL);
            }
        });
        if (logicalBuTbl == null) {
            throw new BadRequestException("Logical BU not found: " + id);
        }
        return toModel(logicalBuTbl);
    }

    @Override
    public LogicalBuModel create(LogicalBuModel model) {
        validateModel(model, true);
        LogicalBuTbl proto = dao.createLocal();
        proto.setName(model.getName().trim());
        proto.setTfsFsId(model.getTfsFsId().trim());
        proto.setActive(model.isActive());
        proto.setDescription(model.getDescription() == null ? "" : model.getDescription());
        proto.setDeleted(false);

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(proto);
            }
        });
        replaceOrgMappings(proto.getId(), model.getCmsOrgIds());
        return findById(proto.getId());
    }

    @Override
    public LogicalBuModel update(long id, LogicalBuModel model) {
        validateModel(model, false);
        LogicalBuTbl existing = queryHandler.handleQuery(new DalQuery<LogicalBuTbl>() {
            @Override
            public LogicalBuTbl doQuery() throws DalException {
                return dao.findById(id, LogicalBuTblEntity.READSET_FULL);
            }
        });
        if (existing == null) {
            throw new BadRequestException("Logical BU not found: " + id);
        }
        existing.setName(model.getName().trim());
        existing.setTfsFsId(model.getTfsFsId().trim());
        existing.setActive(model.isActive());
        existing.setDescription(model.getDescription() == null ? "" : model.getDescription());

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateById(existing, LogicalBuTblEntity.UPDATESET_FULL);
            }
        });
        replaceOrgMappings(id, model.getCmsOrgIds());
        return findById(id);
    }

    @Override
    public void delete(long id) {
        LogicalBuTbl existing = queryHandler.handleQuery(new DalQuery<LogicalBuTbl>() {
            @Override
            public LogicalBuTbl doQuery() throws DalException {
                return dao.findById(id, LogicalBuTblEntity.READSET_FULL);
            }
        });
        if (existing == null) {
            throw new BadRequestException("Logical BU not found: " + id);
        }
        softDeleteOrgMappings(id);
        existing.setKeyId(id);
        queryHandler.handleDelete(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.deleteById(existing, LogicalBuTblEntity.UPDATESET_FULL);
            }
        }, true);
    }

    @Override
    public long resolveLogicalBuIdForCluster(String clusterName, long clusterOrgId) {
        if (clusterOrgId <= 0 || StringUtil.isEmpty(clusterName)) {
            return 0L;
        }
        List<LogicalBuTbl> candidates = queryHandler.handleQuery(new DalQuery<List<LogicalBuTbl>>() {
            @Override
            public List<LogicalBuTbl> doQuery() throws DalException {
                return dao.findActiveByCmsOrgId(clusterOrgId, LogicalBuTblEntity.READSET_FULL);
            }
        });
        if (candidates == null || candidates.isEmpty()) {
            return 0L;
        }
        int idx = Math.floorMod(clusterName.hashCode(), candidates.size());
        return candidates.get(idx).getId();
    }

    private void validateModel(LogicalBuModel model, boolean create) {
        if (model == null) {
            throw new BadRequestException("Logical BU is required");
        }
        if (StringUtil.isEmpty(model.getName())) {
            throw new BadRequestException("Logical BU name is required");
        }
        if (StringUtil.isEmpty(model.getTfsFsId())) {
            throw new BadRequestException("tfs_fs_id is required");
        }
        if (!create && model.getId() <= 0) {
            throw new BadRequestException("Logical BU id is required");
        }
    }

    private void replaceOrgMappings(long logicalBuId, List<Long> cmsOrgIds) {
        softDeleteOrgMappings(logicalBuId);
        if (cmsOrgIds == null || cmsOrgIds.isEmpty()) {
            return;
        }
        LogicalBuOrgTblDao orgDao = logicalBuOrgTblDao;
        for (Long cmsOrgId : cmsOrgIds) {
            if (cmsOrgId == null || cmsOrgId <= 0) {
                continue;
            }
            LogicalBuOrgTbl orgTbl = orgDao.createLocal();
            orgTbl.setLogicalBuId(logicalBuId);
            orgTbl.setCmsOrgId(cmsOrgId);
            orgTbl.setDeleted(false);
            orgTbl.setDeletedAt(0);
            queryHandler.handleInsert(new DalQuery<Integer>() {
                @Override
                public Integer doQuery() throws DalException {
                    return orgDao.insert(orgTbl);
                }
            });
        }
    }

    private void softDeleteOrgMappings(long logicalBuId) {
        LogicalBuOrgTbl proto = logicalBuOrgTblDao.createLocal();
        proto.setTargetLogicalBuId(logicalBuId);
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return logicalBuOrgTblDao.deleteByLogicalBuId(proto, LogicalBuOrgTblEntity.UPDATESET_FULL);
            }
        });
    }

    private LogicalBuModel toModel(LogicalBuTbl tbl) {
        LogicalBuModel model = new LogicalBuModel();
        model.setId(tbl.getId());
        model.setName(tbl.getName());
        model.setTfsFsId(tbl.getTfsFsId());
        model.setActive(tbl.isActive());
        model.setDescription(tbl.getDescription());
        model.setCmsOrgIds(loadCmsOrgIds(tbl.getId()));
        model.setKeeperContainerCount(loadKeeperContainerCount(tbl.getId()));
        return model;
    }

    private List<Long> loadCmsOrgIds(long logicalBuId) {
        List<LogicalBuOrgTbl> orgMappings = queryHandler.handleQuery(new DalQuery<List<LogicalBuOrgTbl>>() {
            @Override
            public List<LogicalBuOrgTbl> doQuery() throws DalException {
                return logicalBuOrgTblDao.findByLogicalBuId(logicalBuId, LogicalBuOrgTblEntity.READSET_FULL);
            }
        });
        if (orgMappings == null || orgMappings.isEmpty()) {
            return Collections.emptyList();
        }
        return orgMappings.stream()
                .map(LogicalBuOrgTbl::getCmsOrgId)
                .collect(Collectors.toList());
    }

    private int loadKeeperContainerCount(long logicalBuId) {
        List<KeepercontainerTbl> keeperContainers = queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
            @Override
            public List<KeepercontainerTbl> doQuery() throws DalException {
                return keepercontainerTblDao.findActiveByLogicalBuId(logicalBuId, KeepercontainerTblEntity.READSET_CONTAINER_ADDRESS);
            }
        });
        return keeperContainers == null ? 0 : keeperContainers.size();
    }
}
