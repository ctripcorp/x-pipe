package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.AppliercontainerCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.*;

@Service
public class AppliercontainerServiceImpl extends AbstractConsoleService<AppliercontainerTblDao>
        implements AppliercontainerService {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private AzService azService;

    @Autowired
    private DcService dcService;

    @Autowired
    private OrganizationService organizationService;

    @Autowired
    private ApplierService applierService;

    @Override
    public AppliercontainerTbl findAppliercontainerTblById(long id) {
        return queryHandler.handleQuery(new DalQuery<AppliercontainerTbl>() {
            @Override
            public AppliercontainerTbl doQuery() throws DalException {
                return dao.findByPK(id, AppliercontainerTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<AppliercontainerTbl> findAllAppliercontainerTblsByDc(final String dcName) {
        DcTbl dcTbl = StringUtil.isEmpty(dcName) ? null : dcService.find(dcName);
        if (null == dcTbl) throw new BadRequestException(String.format("dc %s does not exist", dcName));

        return queryHandler.handleQuery(new DalQuery<List<AppliercontainerTbl>>() {
            @Override
            public List<AppliercontainerTbl> doQuery() throws DalException {
                return dao.findAllAppliercontainersByDc(dcTbl.getId(), AppliercontainerTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<AppliercontainerTbl> findAllAppliercontainersByAz(long azId) {
        return queryHandler.handleQuery(new DalQuery<List<AppliercontainerTbl>>() {
            @Override
            public List<AppliercontainerTbl> doQuery() throws DalException {
                return dao.findAppliercontainersByAz(azId, AppliercontainerTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<AppliercontainerTbl> findAllActiveAppliercontainersByDc(String dcName) {
        DcTbl dcTbl = StringUtil.isEmpty(dcName) ? null : dcService.find(dcName);
        if (null == dcTbl) throw new BadRequestException(String.format("dc %s does not exist", dcName));

        return queryHandler.handleQuery(new DalQuery<List<AppliercontainerTbl>>() {
            @Override
            public List<AppliercontainerTbl> doQuery() throws DalException {
                return dao.findActiveAppliercontainersByDc(dcTbl.getId(), AppliercontainerTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<AppliercontainerTbl> findApplierCount(String dcName) {
        return queryHandler.handleQuery(new DalQuery<List<AppliercontainerTbl>>() {
            @Override
            public List<AppliercontainerTbl> doQuery() throws DalException {
                return dao.findApplierCount(dcName, AppliercontainerTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<AppliercontainerTbl> findBestAppliercontainersByDcCluster(String dcName, String clusterName) {
        long clusterOrgId;
        if (clusterName != null) {
            ClusterTbl clusterTbl = clusterService.find(clusterName);
            clusterOrgId = clusterTbl == null ? XPipeConsoleConstant.DEFAULT_ORG_ID : clusterTbl.getClusterOrgId();
        } else {
            clusterOrgId = XPipeConsoleConstant.DEFAULT_ORG_ID;
        }

        return queryHandler.handleQuery(new DalQuery<List<AppliercontainerTbl>>() {
            @Override
            public List<AppliercontainerTbl> doQuery() throws DalException {
                List<AppliercontainerTbl> appliercontainerTbls = dao.findApplierContainersByDcAndOrg(dcName, clusterOrgId,
                        AppliercontainerTblEntity.READSET_APPLIER_COUNT_BY_CLUSTER);
                if (appliercontainerTbls == null || appliercontainerTbls.isEmpty() ) {
                    logger.info("cluster {} with org id {} is going to find appliercontainers in normal pool",
                            clusterName, clusterOrgId);
                    appliercontainerTbls = dao.findApplierContainersByDcAndOrg(dcName, XPipeConsoleConstant.DEFAULT_ORG_ID,
                            AppliercontainerTblEntity.READSET_APPLIER_COUNT_BY_CLUSTER);
                }
                appliercontainerTbls = filterAppliercontainerFromSameAvailableZone(appliercontainerTbls, dcName);
                logger.info("find applier containers: {}", appliercontainerTbls);
                return appliercontainerTbls;
            }
        });
    }

    private List<AppliercontainerTbl> filterAppliercontainerFromSameAvailableZone(List<AppliercontainerTbl> appliercontainerTbls,
                                                                                  String dcName) {
        List<AzTbl> dcAvailableZones = azService.getDcActiveAvailableZoneTbls(dcName);
        if (dcAvailableZones == null || dcAvailableZones.isEmpty() || dcAvailableZones.size() == 1) {
            return appliercontainerTbls;
        } else {
            Set<Long> usedAvailableZones = new HashSet<>();
            Map<Long, AzTbl> availableZoneMap = new HashMap();
            dcAvailableZones.forEach((availableZone)-> {
                availableZoneMap.put(availableZone.getId(), availableZone);
            });

            List<AppliercontainerTbl> result = new ArrayList<>();
            for (AppliercontainerTbl appliercontainerTbl :appliercontainerTbls) {
                long azId = appliercontainerTbl.getAppliercontainerAz();
                if (!availableZoneMap.containsKey(azId))
                    throw new XpipeRuntimeException(String.format("This appliercontainer %s:%d has unknown available zone id %d ",
                            appliercontainerTbl.getAppliercontainerIp(), appliercontainerTbl.getAppliercontainerPort(), azId));

                if (availableZoneMap.get(azId).isActive() && usedAvailableZones.add(azId)) {
                    result.add(appliercontainerTbl);
                }
            }

            return result;
        }
    }

    public List<AppliercontainerCreateInfo> findAllAppliercontainers() {
        List<AppliercontainerTbl> baseInfos = findAllAppliercontainerTbls();
        Map<Long, String> dcNameMap = dcService.dcNameMap();
        Map<Long, String> azNameMap = azService.azNameMap();
        HashMap<Long, AppliercontainerCreateInfo> containerCreateInfoMap = new HashMap<>();
        baseInfos.forEach(baseInfo -> {
            containerCreateInfoMap.put(baseInfo.getAppliercontainerId(),
                    convertAppliercontainerTblToCreateInfo(baseInfo, dcNameMap, azNameMap));
        });

        return new ArrayList<>(containerCreateInfoMap.values());
    }

    private AppliercontainerCreateInfo convertAppliercontainerTblToCreateInfo(AppliercontainerTbl appliercontainerTbl,
                                                      Map<Long, String> dcNameMap, Map<Long, String> azNameMap) {
        AppliercontainerCreateInfo createInfo = new AppliercontainerCreateInfo();
        createInfo.setId(appliercontainerTbl.getAppliercontainerId())
                .setAppliercontainerIp(appliercontainerTbl.getAppliercontainerIp())
                .setAppliercontainerPort(appliercontainerTbl.getAppliercontainerPort())
                .setAppliercontainerOrgId(appliercontainerTbl.getAppliercontainerOrg())
                .setActive(appliercontainerTbl.isAppliercontainerActive())
                .setDcName(dcNameMap.get(appliercontainerTbl.getAppliercontainerDc()))
                .setAzName(azNameMap.get(appliercontainerTbl.getAppliercontainerAz()));

        OrganizationTbl organizationTbl = organizationService.getOrganization(appliercontainerTbl.getAppliercontainerOrg());
        if (organizationTbl != null) {
            createInfo.setOrgName(organizationTbl.getOrgName()).setAppliercontainerOrgId(organizationTbl.getOrgId());
        } else {
            createInfo.setAppliercontainerOrgId(0L);
        }

        return createInfo;
    }

    private List<AppliercontainerTbl> findAllAppliercontainerTbls() {
        return queryHandler.handleQuery(new DalQuery<List<AppliercontainerTbl>>() {
            @Override
            public List<AppliercontainerTbl> doQuery() throws DalException {
                return dao.findAllApplierContainers(AppliercontainerTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<AppliercontainerCreateInfo> findAllAppliercontainerCreateInfosByDc(String dcName) {
        Map<Long, String> dcNameMap = dcService.dcNameMap();
        Map<Long, String> azNameMap = azService.azNameMap();
        List<AppliercontainerTbl> appliercontainerTbls = findAllAppliercontainerTblsByDc(dcName);

        return Lists.newArrayList(Lists.transform(appliercontainerTbls, new Function<AppliercontainerTbl, AppliercontainerCreateInfo>() {
            @Override
            public AppliercontainerCreateInfo apply(AppliercontainerTbl appliercontainerTbl) {
                return convertAppliercontainerTblToCreateInfo(appliercontainerTbl, dcNameMap, azNameMap);
            }
        }));
    }

    @Override
    public void addAppliercontainerByCreateInfo(AppliercontainerCreateInfo createInfo) {
        AppliercontainerTbl proto = dao.createLocal();

        if(applierContainerAlreadyExists(createInfo.getAppliercontainerIp(), createInfo.getAppliercontainerPort())) {
            throw new IllegalArgumentException("Appliercontainer with IP: "
                    + createInfo.getAppliercontainerIp() + " already exists");
        }

        DcTbl dcTbl = dcService.find(createInfo.getDcName());
        if (dcTbl == null) {
            throw new IllegalArgumentException(String.format("dc name %s dose not exist", createInfo.getDcName()));
        }

        OrganizationTbl org = createInfo.getAppliercontainerOrgId() == 0 ?
            new OrganizationTbl().setOrgId(0L) : organizationService.getOrganization(createInfo.getAppliercontainerOrgId());
        if (org == null) {
            throw new IllegalArgumentException(String.format("org %d does not exist", createInfo.getAppliercontainerOrgId()));
        }

        if (createInfo.getAzName() != null) {
            AzTbl azTbl = azService.getAvailableZoneTblByAzName(createInfo.getAzName());
            if(azTbl == null) {
                throw new IllegalArgumentException(String.format("available zone %s does not exist", createInfo.getAzName()));
            }
            proto.setAppliercontainerAz(azTbl.getId());
        }

        proto.setAppliercontainerDc(dcTbl.getId()).setAppliercontainerIp(createInfo.getAppliercontainerIp())
                .setAppliercontainerPort(createInfo.getAppliercontainerPort()).setAppliercontainerOrg(org.getOrgId())
                .setAppliercontainerActive(createInfo.isActive());

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(proto);
            }
        });
    }

    private boolean applierContainerAlreadyExists(String ip, int port) {
        AppliercontainerTbl existing = findByIpPort(ip, port);
        return existing != null;
    }

    public AppliercontainerTbl findByIpPort(String ip, int port) {
        return queryHandler.handleQuery(new DalQuery<AppliercontainerTbl>() {
            @Override
            public AppliercontainerTbl doQuery() throws DalException {
                return dao.findAppliercontainerByIpPort(ip, port, AppliercontainerTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public void updateAppliercontainerByCreateInfo(AppliercontainerCreateInfo createInfo) {
        AppliercontainerTbl appliercontainerTbl =
                findByIpPort(createInfo.getAppliercontainerIp(), createInfo.getAppliercontainerPort());
        if (null == appliercontainerTbl) {
            throw new IllegalArgumentException(String.format("appliercontainer %s:%d  not found",
                    createInfo.getAppliercontainerIp(), createInfo.getAppliercontainerPort()));
        }

        if (createInfo.getAppliercontainerOrgId() != 0L) {
            OrganizationTbl org = organizationService.getOrganizationTblByCMSOrganiztionId(createInfo.getAppliercontainerOrgId());
            appliercontainerTbl.setAppliercontainerOrg(org.getId());
        } else {
            appliercontainerTbl.setAppliercontainerOrg(0L);
        }

        if (createInfo.getAzName() != null) {
            AzTbl azTbl = azService.getAvailableZoneTblByAzName(createInfo.getAzName());
            if (azTbl == null) {
                throw new IllegalArgumentException(String.format("available zone %s does not exist", createInfo.getAzName()));
            }
            appliercontainerTbl.setAppliercontainerAz(azTbl.getId());
        }

        appliercontainerTbl.setAppliercontainerActive(createInfo.isActive());

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(appliercontainerTbl, AppliercontainerTblEntity.UPDATESET_FULL);
            }
        });
    }

    @Override
    public void deleteAppliercontainerByCreateInfo(String appliercontainerIp, int appliercontainerPort) {
        AppliercontainerTbl appliercontainerTbl = findByIpPort(appliercontainerIp, appliercontainerPort);
        if (null == appliercontainerTbl)
            throw new BadRequestException(String.format("appliercontainer %s:%d does not exist",
                    appliercontainerIp, appliercontainerPort));

        List<ApplierTbl> appliers = applierService.findAllApplierTblsWithSameIp(appliercontainerIp);

        if (appliers != null && !appliers.isEmpty()) {
            throw new BadRequestException(String.format("This appliercontainer %s:%d is not empty, unable to delete",
                    appliercontainerIp, appliercontainerPort));
        }

        AppliercontainerTbl proto = appliercontainerTbl;
        queryHandler.handleDelete(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.deleteApplierContainer(proto, AppliercontainerTblEntity.UPDATESET_FULL);
            }
        }, true);
    }

    public List<AppliercontainerInfoModel> findAllAppliercontainerInfoModels() {
        List<AppliercontainerTbl> appliercontainerTbls = findAllAppliercontainerTbls();
        Map<Long, String> dcNameMap = dcService.dcNameMap();
        Map<Long, String> azNameMap = azService.azNameMap();
        HashMap<Long, AppliercontainerInfoModel> containerCreateInfoMap = new HashMap<>();
        appliercontainerTbls.forEach(appliercontainerTbl -> {
            containerCreateInfoMap.put(appliercontainerTbl.getAppliercontainerId(),
                    convertAppliercontainerTblToInfoModel(appliercontainerTbl, dcNameMap, azNameMap));
        });

        List<ApplierTbl> containerCountInfo = applierService.findAllAppliercontainerCountInfo();
        containerCountInfo.forEach(info -> {
            if (!containerCreateInfoMap.containsKey(info.getContainerId())) return;
            AppliercontainerInfoModel model = containerCreateInfoMap.get(info.getContainerId());
            model.setApplierCount(info.getCount());
            model.setClusterCount(info.getDcClusterShardInfo().getClusterCount());
            model.setShardCount(info.getDcClusterShardInfo().getShardCount());
        });

        return new ArrayList<>(containerCreateInfoMap.values());
    }

    @Override
    public AppliercontainerInfoModel findAppliercontainerInfoModelById(long appliercontainerId) {
        return convertAppliercontainerTblToInfoModel(findAppliercontainerTblById(appliercontainerId), dcService.dcNameMap(), azService.azNameMap());
    }

    private AppliercontainerInfoModel convertAppliercontainerTblToInfoModel(AppliercontainerTbl appliercontainerTbl,
                                                    Map<Long, String> dcNameMap, Map<Long, String> azNameMap) {
        AppliercontainerInfoModel infoModel = new AppliercontainerInfoModel();
        if (null == appliercontainerTbl) return infoModel;
        infoModel.setId(appliercontainerTbl.getAppliercontainerId()).setActive(appliercontainerTbl.isAppliercontainerActive())
                .setAddr(new HostPort(appliercontainerTbl.getAppliercontainerIp(), appliercontainerTbl.getAppliercontainerPort()))
                .setDcName(dcNameMap.get(appliercontainerTbl.getAppliercontainerDc()))
                .setAzName(azNameMap.get(appliercontainerTbl.getAppliercontainerAz()));

        OrganizationTbl organizationTbl = organizationService.getOrganization(appliercontainerTbl.getAppliercontainerOrg());
        if (organizationTbl != null) {
            infoModel.setOrgName(organizationTbl.getOrgName());
        }

        return infoModel;
    }

    public void addAppliercontainerByInfoModel(AppliercontainerInfoModel infoModel) {
        if(applierContainerAlreadyExists(infoModel.getAddr().getHost(), infoModel.getAddr().getPort())) {
            throw new IllegalArgumentException("Appliercontainer with IP: "
                    + infoModel.getAddr().getHost() + " already exists");
        }

        AppliercontainerTbl proto = dao.createLocal();

        DcTbl dcTbl = dcService.find(infoModel.getDcName());
        if (dcTbl == null) {
            throw new IllegalArgumentException(String.format("dc name %s dose not exist", infoModel.getDcName()));
        }
        proto.setAppliercontainerDc(dcTbl.getId());

        OrganizationTbl org;
        if (infoModel.getOrgName() == null || (org = organizationService.getOrgByName(infoModel.getOrgName())) == null) {
            proto.setAppliercontainerOrg(0L);
        } else {
            proto.setAppliercontainerOrg(org.getId());
        }

        if (infoModel.getAzName() != null) {
            AzTbl azTbl = azService.getAvailableZoneTblByAzName(infoModel.getAzName());
            if(azTbl == null) {
                throw new IllegalArgumentException(String.format("available zone %s does not exist", infoModel.getAzName()));
            }
            proto.setAppliercontainerAz(azTbl.getId());
        }

        proto.setAppliercontainerIp(infoModel.getAddr().getHost())
                .setAppliercontainerPort(infoModel.getAddr().getPort())
                .setAppliercontainerActive(infoModel.isActive());

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(proto);
            }
        });
    }

    @Override
    public void updateAppliercontainerByInfoModel(AppliercontainerInfoModel infoModel) {
        AppliercontainerTbl proto = findByIpPort(infoModel.getAddr().getHost(), infoModel.getAddr().getPort());
        if( proto == null) {
            throw new IllegalArgumentException("Appliercontainer with IP: " + infoModel.getAddr().getHost()
                    + " does not exists");
        }

        OrganizationTbl org;
        if (infoModel.getOrgName() == null || (org = organizationService.getOrgByName(infoModel.getOrgName())) == null) {
            proto.setAppliercontainerOrg(0L);
        } else {
            proto.setAppliercontainerOrg(org.getId());
        }

        proto.setAppliercontainerActive(infoModel.isActive());

        if (infoModel.getAzName() != null) {
            AzTbl azTbl = azService.getAvailableZoneTblByAzName(infoModel.getAzName());
            if(azTbl == null) {
                throw new IllegalArgumentException(String.format("available zone %s does not exist", infoModel.getAzName()));
            }
            proto.setAppliercontainerAz(azTbl.getId());
        }

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(proto, AppliercontainerTblEntity.UPDATESET_FULL);
            }
        });
    }
}
