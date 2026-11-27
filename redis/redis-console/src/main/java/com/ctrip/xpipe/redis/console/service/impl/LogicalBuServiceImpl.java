package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.utils.StringUtil;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(false)
public class LogicalBuServiceImpl extends AbstractConsoleService<LogicalBuTblDao> implements LogicalBuService {

    private LogicalBuOrgTblDao logicalBuOrgTblDao;

    private KeepercontainerTblDao keepercontainerTblDao;

    @Autowired
    private DcService dcService;

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
        // one DC_TBL read per request, shared by every BU row
        Map<Long, String> dcNames = loadDcNames();
        return all.stream().map(tbl -> toModel(tbl, dcNames)).collect(Collectors.toList());
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
        LogicalBuModel model = toModel(logicalBuTbl, loadDcNames());
        model.setKeeperContainers(loadKeeperContainers(id));
        return model;
    }

    @Override
    @Transactional
    @DalTransaction
    public LogicalBuModel create(LogicalBuModel model) {
        validateModel(model, true);
        LogicalBuTbl proto = dao.createLocal();
        proto.setName(model.getName().trim());
        proto.setTfsFsId(model.getTfsFsId().trim());
        proto.setActive(model.isActive());
        proto.setDescription(model.getDescription() == null ? "" : model.getDescription());
        proto.setDeleted(false);
        // Unidal INSERT always binds DataChange_LastTime; datetime rejects explicit NULL (D27)
        proto.setDataChangeLastTime(new Date());

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
    @Transactional
    @DalTransaction
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
                // BASIC excludes priority: portal edits must not overwrite a concurrent keeper-ops switch
                return dao.updateById(existing, LogicalBuTblEntity.UPDATESET_BASIC);
            }
        });
        replaceOrgMappings(id, model.getCmsOrgIds());
        return findById(id);
    }

    @Override
    public Map<Long, Integer> findAllPriorities() {
        List<LogicalBuTbl> all = queryHandler.handleQuery(new DalQuery<List<LogicalBuTbl>>() {
            @Override
            public List<LogicalBuTbl> doQuery() throws DalException {
                return dao.findAll(LogicalBuTblEntity.READSET_PRIORITY);
            }
        });
        if (all == null || all.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Long, Integer> result = new HashMap<>();
        for (LogicalBuTbl tbl : all) {
            if (tbl.getPriority() != null) {
                result.put(tbl.getId(), tbl.getPriority());
            }
        }
        return result;
    }

    @Override
    public boolean casPriority(long id, Integer expected, Integer target) {
        LogicalBuTbl proto = dao.createLocal();
        proto.setKeyId(id);
        proto.setPriority(target);
        proto.setExpectedPriority(expected);
        // Must not go through queryHandler.handleUpdate: it treats "0 rows" as a failure and hides the CAS result.
        try {
            int updated = expected == null
                    ? dao.updatePriorityIfNull(proto, LogicalBuTblEntity.UPDATESET_FULL)
                    : dao.updatePriorityIfEquals(proto, LogicalBuTblEntity.UPDATESET_FULL);
            return updated == 1;
        } catch (DalException e) {
            throw new ServerException("Update logical bu priority failed." + e.getMessage(), e);
        }
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
        List<LogicalBuOrgTbl> toInsert = new ArrayList<>();
        Date now = new Date();
        for (Long cmsOrgId : cmsOrgIds) {
            if (cmsOrgId == null || cmsOrgId <= 0) {
                continue;
            }
            LogicalBuOrgTbl orgTbl = logicalBuOrgTblDao.createLocal();
            orgTbl.setLogicalBuId(logicalBuId);
            orgTbl.setCmsOrgId(cmsOrgId);
            orgTbl.setDeleted(false);
            orgTbl.setDeletedAt(0);
            // Unidal INSERT always binds DataChange_LastTime; datetime rejects explicit NULL (D27)
            orgTbl.setDataChangeLastTime(now);
            toInsert.add(orgTbl);
        }
        if (toInsert.isEmpty()) {
            return;
        }
        LogicalBuOrgTbl[] protos = toInsert.toArray(new LogicalBuOrgTbl[0]);
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                int[] results = logicalBuOrgTblDao.insertBatch(protos);
                return results == null ? 0 : results.length;
            }
        });
    }

    private void softDeleteOrgMappings(long logicalBuId) {
        LogicalBuOrgTbl proto = logicalBuOrgTblDao.createLocal();
        proto.setTargetLogicalBuId(logicalBuId);
        queryHandler.handleDelete(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return logicalBuOrgTblDao.deleteByLogicalBuId(proto, LogicalBuOrgTblEntity.UPDATESET_FULL);
            }
        }, true);
    }

    private LogicalBuModel toModel(LogicalBuTbl tbl, Map<Long, String> dcNames) {
        LogicalBuModel model = new LogicalBuModel();
        model.setId(tbl.getId());
        model.setName(tbl.getName());
        model.setTfsFsId(tbl.getTfsFsId());
        model.setPriority(tbl.getPriority());
        model.setActive(tbl.isActive());
        model.setDescription(tbl.getDescription());
        model.setCmsOrgIds(loadCmsOrgIds(tbl.getId()));
        fillKeeperContainerStats(model, loadKeeperContainerTbls(tbl.getId()), dcNames);
        return model;
    }

    private Map<Long, String> loadDcNames() {
        Map<Long, String> dcNames = dcService.dcNameMap();
        return dcNames == null ? Collections.emptyMap() : dcNames;
    }

    /** Total and per-idc counts of the BU's active keepercontainers; the BU's idcs are where those containers are. */
    private void fillKeeperContainerStats(LogicalBuModel model, List<KeepercontainerTbl> containers,
                                          Map<Long, String> dcNames) {
        Set<Long> seen = new HashSet<>();
        Map<String, Integer> perIdc = new TreeMap<>();
        if (containers != null) {
            for (KeepercontainerTbl container : containers) {
                if (!seen.add(container.getKeepercontainerId())) {
                    continue;
                }
                String idc = dcNames.get(container.getKeepercontainerDc());
                if (idc == null) {
                    idc = String.valueOf(container.getKeepercontainerDc());
                }
                perIdc.merge(idc, 1, Integer::sum);
            }
        }
        model.setKeeperContainerCount(seen.size());
        model.setIdcKeeperContainerCounts(new LinkedHashMap<>(perIdc));
        model.setIdcNames(new ArrayList<>(perIdc.keySet()));
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

    private List<LogicalBuKeeperContainerModel> loadKeeperContainers(long logicalBuId) {
        List<KeepercontainerTbl> keeperContainers = loadKeeperContainerTbls(logicalBuId);
        if (keeperContainers == null || keeperContainers.isEmpty()) {
            return Collections.emptyList();
        }
        return keeperContainers.stream().map(this::toKeeperContainerModel).collect(Collectors.toList());
    }

    private List<KeepercontainerTbl> loadKeeperContainerTbls(long logicalBuId) {
        return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
            @Override
            public List<KeepercontainerTbl> doQuery() throws DalException {
                return keepercontainerTblDao.findActiveByLogicalBuId(logicalBuId, KeepercontainerTblEntity.READSET_FULL);
            }
        });
    }

    private LogicalBuKeeperContainerModel toKeeperContainerModel(KeepercontainerTbl tbl) {
        return new LogicalBuKeeperContainerModel()
                .setId(tbl.getKeepercontainerId())
                .setIp(tbl.getKeepercontainerIp())
                .setPort(tbl.getKeepercontainerPort())
                .setActive(tbl.isKeepercontainerActive())
                .setDiskType(tbl.getKeepercontainerDiskType())
                .setTag(tbl.getTag());
    }
}
