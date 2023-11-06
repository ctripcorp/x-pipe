package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.DcClusterDao;
import com.ctrip.xpipe.redis.console.dao.RouteDao;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.dto.AzGroupDTO;
import com.ctrip.xpipe.redis.console.dto.ClusterCreateDTO;
import com.ctrip.xpipe.redis.console.dto.ClusterDTO;
import com.ctrip.xpipe.redis.console.dto.ClusterUpdateDTO;
import com.ctrip.xpipe.redis.console.dto.MultiGroupClusterCreateDTO;
import com.ctrip.xpipe.redis.console.dto.SingleGroupClusterCreateDTO;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.entity.DcClusterEntity;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListUnhealthyClusterModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyChainModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterDeleteEventFactory;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterEvent;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.repository.AzGroupRepository;
import com.ctrip.xpipe.redis.console.repository.DcClusterRepository;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.redis.console.service.model.SourceModelService;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.APPLIER_PORT_DEFAULT;
import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;

@Service
public class ClusterServiceImpl extends AbstractConsoleService<ClusterTblDao> implements ClusterService {

	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterDao clusterDao;
	@Autowired
	private DcClusterDao dcClusterDao;
	@Autowired
	private ShardDao shardDao;
	@Autowired
	private RouteDao routeDao;

	@Autowired
	private ApplierService applierService;
	@Autowired
	private ClusterMetaModifiedNotifier notifier;
	@Autowired
	private ShardService shardService;
	@Autowired
	private OrganizationService organizationService;
	@Autowired
	private DcClusterShardService dcClusterShardService;
	@Autowired
	private DelayService delayService;
	@Autowired
	private RouteService routeService;
	@Autowired
	private ProxyService proxyService;
	@Autowired
	private MetaCache metaCache;

	@Autowired
	private ClusterDeleteEventFactory clusterDeleteEventFactory;

	@Autowired
	private DcClusterService dcClusterService;

	@Autowired
	private RedisService redisService;

	@Autowired
	private SentinelBalanceService sentinelBalanceService;

	@Autowired
	private ConsoleConfig consoleConfig;

	@Autowired
	private ReplDirectionService replDirectionService;

	@Autowired
	private SourceModelService sourceModelService;

	@Autowired
	private ShardModelService shardModelService;

	@Autowired
	private KeeperAdvancedService keeperAdvancedService;

    @Autowired
    private AzGroupCache azGroupCache;

	@Autowired
	private DcClusterRepository dcClusterRepository;

	@Autowired
	private AzGroupRepository azGroupRepository;

	@Autowired
	private AzGroupClusterRepository azGroupClusterRepository;

	private static final String DESIGNATED_ROUTE_ID_SPLITTER = "\\s*,\\s*";

	private static final String PROXY_SPLITTER = "\\s*-\\s*";
	private static final String PROXYTCP = "PROXYTCP://%s:80";
	private static final String PROXYTLS = "PROXYTLS://%s:443";

	@Override
	public ClusterTbl find(final String clusterName) {
		return clusterDao.findClusterByClusterName(clusterName);
	}

	@Override
	public List<ClusterTbl> findClustersByGroupType(final String groupType) {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findClustersByGroupType(groupType, ClusterTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<ClusterTbl> findAllByNames(List<String> clusterNames) {
		return clusterDao.findClustersWithName(clusterNames);
	}

	@Override
	public ClusterStatus clusterStatus(String clusterName) {

		ClusterTbl clusterTbl = find(clusterName);
		if (clusterTbl == null) {
			throw new IllegalArgumentException("cluster not found:" + clusterName);
		}
		return ClusterStatus.valueOf(clusterTbl.getStatus());
	}

	@Override
	public List<DcTbl> getClusterRelatedDcs(String clusterName) {
		ClusterTbl clusterTbl = find(clusterName);
		if (null == clusterTbl) {
			throw new IllegalArgumentException("cluster not found:" + clusterName);
		}

		List<DcClusterTbl> dcClusterTbls = dcClusterService.findClusterRelated(clusterTbl.getId());
		List<DcTbl> result = Lists.newLinkedList();
		for (DcClusterTbl dcClusterTbl : dcClusterTbls) {
			result.add(dcService.find(dcClusterTbl.getDcId()));
		}
		return result;
	}

    @Override
    public boolean containsRedisInstance(String clusterName) {
        List<DcTbl> dcs = this.getClusterRelatedDcs(clusterName);
        for (DcTbl dc: dcs) {
            String dcName = dc.getDcName();
            List<RedisTbl> redises = redisService.findAllRedisesByDcClusterName(dcName, clusterName);
			if (!CollectionUtils.isEmpty(redises)) {
				return true;
			}
        }
        return false;
    }

    @Override
	public ClusterTbl find(final long clusterId) {
		return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findByPK(clusterId, ClusterTblEntity.READSET_FULL);
			}

		});
	}


	@Override
	public List<String> findAllClusterNames() {

		List<ClusterTbl> clusterTbls = queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_NAME);
			}
		});

		List<String> clusterNames = new ArrayList<>(clusterTbls.size());

		clusterTbls.forEach(clusterTbl -> clusterNames.add(clusterTbl.getClusterName()));

		return clusterNames;
	}

	@Override
	public Map<String, Long> getAllCountByActiveDc() {
		List<DcTbl> dcs = dcService.findAllDcs();
		Map<String, Long> counts = new HashMap<>();

		dcs.forEach(dcTbl -> {
			counts.put(dcTbl.getDcName(), getCountByActiveDc(dcTbl.getId()));
		});

		return counts;
	}

	@Override
	public Long getCountByActiveDc(long activeDc) {
		return queryHandler.handleQuery(new DalQuery<Long>() {
			@Override
			public Long doQuery() throws DalException {
				return dao.countByActiveDc(activeDc, ClusterTblEntity.READSET_COUNT).getCount();
			}
		});
	}

	@Override
	public Long getCountByActiveDcAndClusterType(long activeDc, String clusterType) {
		return queryHandler.handleQuery(new DalQuery<Long>() {
			@Override
			public Long doQuery() throws DalException {
				return dao.countByActiveDcAndClusterType(activeDc, clusterType, ClusterTblEntity.READSET_COUNT).getCount();
			}
		});
	}

	@Override
	public Long getAllCount() {
		return queryHandler.handleQuery(new DalQuery<Long>() {
			@Override
			public Long doQuery() throws DalException {
				return dao.totalCount(ClusterTblEntity.READSET_COUNT).getCount();
			}
		});
	}

	@Override
	public ClusterDTO getCluster(String clusterName) {
        ClusterTbl clusterTbl = findClusterAndOrg(clusterName);
        ClusterDTO cluster = new ClusterDTO(clusterTbl);
        if (clusterTbl.getOrganizationInfo() != null) {
            cluster.setCmsOrgId(clusterTbl.getOrganizationInfo().getOrgId());
        } else {
            cluster.setCmsOrgId(0L);
        }

        Map<Long, String> dcIdNameMap = dcService.dcNameMap();
        cluster.setActiveAz(dcIdNameMap.get(clusterTbl.getActivedcId()));

        List<DcClusterEntity> dcClusters = dcClusterRepository.selectByClusterId(clusterTbl.getId());
        List<String> dcNames = dcClusters.stream()
            .map(dcCluster -> dcIdNameMap.get(dcCluster.getDcId()))
            .collect(Collectors.toList());
        cluster.setAzs(dcNames);

        List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(clusterTbl.getId());
        List<AzGroupDTO> azGroups = azGroupClusters.stream()
            .map(azGroupCluster -> {
                AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
                return AzGroupDTO.builder()
                    .region(azGroup.getRegion())
                    .clusterType(azGroupCluster.getAzGroupClusterType())
                    .activeAz(dcIdNameMap.get(azGroupCluster.getActiveAzId()))
                    .azs(azGroup.getAzsAsList())
                    .build();
            })
            .collect(Collectors.toList());
        cluster.setAzGroups(azGroups);

        return cluster;
    }

	@Override
	public List<ClusterDTO> getClusters(String clusterType) {
        Map<Long, String> dcIdNameMap = dcService.dcNameMap();
        List<ClusterTbl> clusterTbls = findClustersWithOrgInfoByClusterType(clusterType);
        List<Long> clusterIds = clusterTbls.stream().map(ClusterTbl::getId).collect(Collectors.toList());

        List<DcClusterEntity> dcClusters = dcClusterRepository.selectByClusterIds(clusterIds);
        Map<Long, List<String>> clusterIdDcsMap = dcClusters.stream()
            .collect(Collectors.groupingBy(DcClusterEntity::getClusterId,
                Collectors.mapping(dcCluster -> dcIdNameMap.get(dcCluster.getDcId()), Collectors.toList())));
        List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterIds(clusterIds);
        Map<Long, List<AzGroupClusterEntity>> clusterIdAzGroupClusterMap = azGroupClusters.stream()
            .collect(Collectors.groupingBy(AzGroupClusterEntity::getClusterId));

        List<ClusterDTO> clusters = new ArrayList<>();
        for (ClusterTbl clusterTbl : clusterTbls) {
            ClusterDTO cluster = new ClusterDTO(clusterTbl);
            if (clusterTbl.getOrganizationInfo() != null) {
                cluster.setCmsOrgId(clusterTbl.getOrganizationInfo().getOrgId());
            } else {
                cluster.setCmsOrgId(0L);
            }
            cluster.setActiveAz(dcIdNameMap.get(clusterTbl.getActivedcId()));

            List<String> azs = clusterIdDcsMap.getOrDefault(clusterTbl.getId(), Collections.emptyList());
            cluster.setAzs(azs);

            List<AzGroupClusterEntity> clusterAzGroups = clusterIdAzGroupClusterMap.get(clusterTbl.getId());
			if (!CollectionUtils.isEmpty(clusterAzGroups)) {
				List<AzGroupDTO> azGroups = clusterAzGroups.stream()
					.map(agc -> {
                        AzGroupModel azGroup = azGroupCache.getAzGroupById(agc.getAzGroupId());
                        return AzGroupDTO.builder()
                            .region(azGroup.getRegion())
                            .clusterType(agc.getAzGroupClusterType())
                            .activeAz(dcIdNameMap.get(agc.getActiveAzId()))
                            .azs(azGroup.getAzsAsList())
                            .build();
                    })
					.collect(Collectors.toList());
				cluster.setAzGroups(azGroups);
			}

			clusters.add(cluster);
        }

        return clusters;
	}

    @Override
	@Transactional
    @DalTransaction
    public synchronized ClusterTbl createCluster(ClusterModel clusterModel) {
        ClusterTbl cluster = clusterModel.getClusterTbl();
        List<DcTbl> allDcs = clusterModel.getDcs();
        List<ShardModel> shards = clusterModel.getShards();
        ClusterType clusterType = ClusterType.lookup(cluster.getClusterType());
        List<ReplDirectionInfoModel> replDirections = clusterModel.getReplDirections();
        List<DcClusterModel> dcClusters = clusterModel.getDcClusters();

        // ensure active dc assigned
        if (!clusterType.supportMultiActiveDC() && XPipeConsoleConstant.NO_ACTIVE_AZ_TAG == cluster.getActivedcId()) {
            throw new BadRequestException("No active dc assigned.");
        }
        ClusterTbl proto = dao.createLocal();
        proto.setClusterName(cluster.getClusterName().trim());
        proto.setClusterType(cluster.getClusterType());
        proto.setClusterDescription(cluster.getClusterDescription());
        proto.setStatus(ClusterStatus.Normal.toString());
        proto.setIsXpipeInterested(true);
        proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
        proto.setClusterDesignatedRouteIds(cluster.getClusterDesignatedRouteIds() == null ? "" : cluster.getClusterDesignatedRouteIds());
        if (clusterType.supportMultiActiveDC()) {
            proto.setActivedcId(0L);
        } else {
            proto.setActivedcId(cluster.getActivedcId());
        }
        if (!checkEmails(cluster.getClusterAdminEmails())) {
            throw new IllegalArgumentException("Emails should be ctrip emails and separated by comma or semicolon");
        }
        proto.setClusterAdminEmails(cluster.getClusterAdminEmails());
        proto.setClusterOrgId(getOrgIdFromClusterOrgName(cluster));

        final ClusterTbl queryProto = proto;
        ClusterTbl result = queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
            @Override
            public ClusterTbl doQuery() throws DalException {
                return clusterDao.createCluster(queryProto);
            }
        });

        if (dcClusters != null) {
            for (DcClusterModel dcCluster : dcClusters) {
                DcTbl dcTbl = dcService.find(dcCluster.getDc().getDc_name());
                if (dcTbl == null) {
                    throw new BadRequestException(String.format("dc %s does not exist", dcCluster.getDc().getDc_name()));
                }
                dcCluster.getDcCluster().setDcId(dcTbl.getId());
                DcClusterTbl dcClusterInfo = dcCluster.getDcCluster();
                DcClusterTbl dcProto = dcClusterInfo == null ? new DcClusterTbl() : dcClusterInfo;
                dcProto.setClusterName(result.getClusterName()).setDcName(dcCluster.getDc().getDc_name());
                bindDc(dcProto);
            }

            for (DcClusterModel dcCluster : dcClusters) {
                if ((DcGroupType.isNullOrDrMaster(dcCluster.getDcCluster().getGroupType()))
                    && dcCluster.getDcCluster().getDcId() != result.getActivedcId()) continue;

                if (dcCluster.getShards() != null && !dcCluster.getShards().isEmpty()) {
                    List<DcClusterTbl> dcClusterTbls =
                        dcClusterService.findAllByClusterAndGroupType(result.getId(),
                            dcCluster.getDcCluster().getDcId(), dcCluster.getDcCluster().getGroupType());

                    dcCluster.getShards().forEach(shardModel -> {
                        shardService.findOrCreateShardIfNotExist(result.getClusterName(), shardModel.getShardTbl(),
                            dcClusterTbls, sentinelBalanceService.selectMultiDcSentinels(clusterType));
                    });
                }
            }
        }

        if (shards != null) {
            for (ShardModel shard : shards) {
                shardService.createShard(result.getClusterName(), shard.getShardTbl(), shard.getSentinels());
            }
        }

        if (replDirections != null) {
            for (ReplDirectionInfoModel replDirection : replDirections) {
                replDirectionService.addReplDirectionByInfoModel(result.getClusterName(), replDirection);
            }
        }

        return result;
    }

	@Transactional
	@DalTransaction
	public void createSingleGroupCluster(SingleGroupClusterCreateDTO clusterCreateDTO) {
		ClusterTbl clusterTbl = createCluster(clusterCreateDTO);

		List<String> azs = clusterCreateDTO.getAzs();
		for (String az : azs) {
			DcTbl dcTbl = dcService.find(az);
			if (dcTbl == null) {
				throw new BadRequestException(String.format("az - %s does not exist", az));
			}
			DcClusterEntity dcCluster = new DcClusterEntity();
			dcCluster.setClusterId(clusterTbl.getId());
			dcCluster.setDcId(dcTbl.getId());
			dcClusterRepository.insert(dcCluster);
		}
	}

	@Transactional
	@DalTransaction
	public void createMultiGroupCluster(MultiGroupClusterCreateDTO clusterCreateDTO) {
		ClusterTbl clusterTbl = createCluster(clusterCreateDTO);

		List<AzGroupDTO> azGroups = clusterCreateDTO.getAzGroups();
		for (AzGroupDTO azGroupDTO : azGroups) {
			List<String> azs = azGroupDTO.getAzs();
			Map<String, Long> azIdMap = new HashMap<>();
			for (String az : azs) {
				DcTbl dcTbl = dcService.find(az);
				if (dcTbl == null) {
					throw new BadRequestException(String.format("az - %s does not exist", az));
				}
				azIdMap.put(az, dcTbl.getId());
			}

			AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity();
			azGroupCluster.setClusterId(clusterTbl.getId());

            String region = azGroupDTO.getRegion();
            AzGroupModel azGroup = azGroupCache.getAzGroupByAzs(azs);
			if (azGroup == null || !Objects.equals(azGroup.getRegion(), region)) {
                throw new BadRequestException(
                    String.format("Region: %s doesn't contain such azs: %s", region, azs));
			}
			azGroupCluster.setAzGroupId(azGroup.getId());

			ClusterType clusterType = ClusterType.lookup(azGroupDTO.getClusterType());
			azGroupCluster.setAzGroupClusterType(clusterType.toString());

			if (clusterType.supportMultiActiveDC()) {
				azGroupCluster.setActiveAzId(XPipeConsoleConstant.NO_ACTIVE_AZ_TAG);
			} else {
				String activeAz = azGroupDTO.getActiveAz();
				if (!azIdMap.containsKey(activeAz)) {
					throw new BadRequestException(String.format("active az - %s not in azs - %s", activeAz, azs));
				}
				azGroupCluster.setActiveAzId(azIdMap.get(activeAz));
			}
			azGroupClusterRepository.insert(azGroupCluster);

			for (String az : azs) {
				DcClusterEntity dcCluster = new DcClusterEntity();
				dcCluster.setClusterId(clusterTbl.getId());
				dcCluster.setDcId(azIdMap.get(az));
				dcCluster.setAzGroupClusterId(azGroupCluster.getId());
				dcClusterRepository.insert(dcCluster);
			}
		}
	}

	private ClusterTbl createCluster(ClusterCreateDTO clusterCreateDTO) {
		String clusterName = clusterCreateDTO.getClusterName().trim();
		ClusterTbl clusterTbl = clusterDao.findClusterByClusterName(clusterName);
		if (clusterTbl != null) {
			throw new BadRequestException(String.format("cluster - %s exist", clusterName));
		}

		clusterTbl = new ClusterTbl();
		clusterTbl.setClusterName(clusterName);
		clusterTbl.setClusterDescription(clusterCreateDTO.getDescription());

		ClusterType clusterType = ClusterType.lookup(clusterCreateDTO.getClusterType());
		clusterTbl.setClusterType(clusterType.toString());

		if (clusterType.supportMultiActiveDC()) {
			clusterTbl.setActivedcId(XPipeConsoleConstant.NO_ACTIVE_AZ_TAG);
		} else {
			String activeAz = clusterCreateDTO.getActiveAz();
			DcTbl dcTbl = dcService.find(activeAz);
			if (dcTbl == null) {
				throw new BadRequestException(String.format("az - %s does not exist", activeAz));
			}
			clusterTbl.setActivedcId(dcTbl.getId());
		}

		String orgName = clusterCreateDTO.getOrgName();
		OrganizationTbl orgTbl = organizationService.getOrgByName(orgName);
		long orgId = orgTbl == null ? 0L : orgTbl.getId();
		clusterTbl.setClusterOrgId(orgId);

		String clusterAdminEmails = clusterCreateDTO.getAdminEmails();
		if (!checkEmails(clusterAdminEmails)) {
			throw new IllegalArgumentException("Emails should be ctrip emails and separated by comma or semicolon");
		}
		clusterTbl.setClusterAdminEmails(clusterAdminEmails);

		clusterTbl.setStatus(ClusterStatus.Normal.toString());
		clusterTbl.setIsXpipeInterested(true);
		clusterTbl.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		clusterTbl.setClusterDesignatedRouteIds("");

		return clusterDao.createCluster(clusterTbl);
	}

	public long getOrgIdFromClusterOrgName(ClusterTbl cluster) {
		String orgName = cluster.getClusterOrgName();
		OrganizationTbl organizationTbl = organizationService.getOrgByName(orgName);
		if (organizationTbl == null)
			return 0L;
		Long id = organizationTbl.getId();
		return id == null ? 0L : id;
	}

	@Override
	public ClusterTbl findClusterAndOrg(String clusterName) {
		ClusterTbl clusterTbl = clusterDao.findClusterAndOrgByName(clusterName);
		OrganizationTbl organizationTbl = clusterTbl.getOrganizationInfo();
		if (organizationTbl != null) {
			clusterTbl.setClusterOrgName(organizationTbl.getOrgName());
		}
		// Set null if no organization bind with cluster
		if (organizationTbl == null || organizationTbl.getId() == null) {
			clusterTbl.setOrganizationInfo(null);
		}
		return clusterTbl;
	}

	@Override
	public Map<String, Long> getMigratableClustersCountByActiveDc() {
		List<DcTbl> dcs = dcService.findAllDcs();
		Map<String, Long> counts = new HashMap<>();

		dcs.forEach(dcTbl -> {
			counts.put(dcTbl.getDcName(), getMigratableClustersCountByActiveDcId(dcTbl.getId()));
		});

		return counts;
	}

	public long getMigratableClustersCountByActiveDcId(long activeDc) {
		List<ClusterTbl> dcClusters = findAllClustersByActiveDcId(activeDc);
		int count = 0;
		for (ClusterTbl clusterTbl : dcClusters) {
			if (ClusterType.lookup(clusterTbl.getClusterType()).supportMigration())
				count++;
		}
		return count;
	}

	public List<ClusterTbl> findAllClustersByActiveDcId(long activeDc) {
		return clusterDao.findClustersByActiveDcId(activeDc);
	}

	@Override
	public List<ClusterTbl> findClustersWithOrgInfoByActiveDcId(long activeDc) {
		List<ClusterTbl> result = clusterDao.findClustersWithOrgInfoByActiveDcId(activeDc);
		result = fillClusterOrgName(result);
		return setOrgNullIfNoOrgIdExsits(result);
	}

	@Override
	public List<ClusterTbl> findAllClustersWithOrgInfo() {
		List<ClusterTbl> result = clusterDao.findAllClusterWithOrgInfo();
		result = fillClusterOrgName(result);
		return setOrgNullIfNoOrgIdExsits(result);
	}

	@Override
	public List<ClusterTbl> findClustersWithOrgInfoByClusterType(String clusterType) {
		List<ClusterTbl> result = clusterDao.findClusterWithOrgInfoByClusterType(clusterType);
		result = fillClusterOrgName(result);
		return setOrgNullIfNoOrgIdExsits(result);
	}

	private List<ClusterTbl> fillClusterOrgName(List<ClusterTbl> clusterTblList) {
		for (ClusterTbl cluster : clusterTblList) {
			cluster.setClusterOrgName(cluster.getOrganizationInfo().getOrgName());
		}
		return clusterTblList;
	}

	private List<ClusterTbl> setOrgNullIfNoOrgIdExsits(List<ClusterTbl> clusterTblList) {
		for (ClusterTbl cluster : clusterTblList) {
			OrganizationTbl organizationTbl = cluster.getOrganizationInfo();
			if (organizationTbl.getId() == null) {
				cluster.setOrganizationInfo(null);
			}
		}
		return clusterTblList;
	}

	@Override
	public String updateCluster(ClusterUpdateDTO clusterUpdateDTO) {
		String clusterName = clusterUpdateDTO.getClusterName();
		ClusterTbl clusterTbl = clusterDao.findClusterByClusterName(clusterName);
		if (clusterTbl == null) {
			throw new BadRequestException("Not find cluster: " + clusterName);
		}

        boolean needUpdate = false;
        ClusterType clusterType = StringUtils.isEmpty(clusterUpdateDTO.getClusterType())
			? null : ClusterType.lookup(clusterUpdateDTO.getClusterType());
        if (clusterType != null) {
            String oldClusterType = clusterTbl.getClusterType();
            if (!ClusterType.isSameClusterType(oldClusterType, clusterType)) {
                if (ClusterType.isSameClusterType(oldClusterType, ClusterType.ONE_WAY)
                    && clusterType == ClusterType.HETERO) {
                    needUpdate = true;
                    clusterTbl.setClusterType(clusterType.toString());
                } else {
                    // 仅允许单向同步集群改为异构集群
                    throw new BadRequestException("Only ONE_WAY cluster can change type to HETERO");
                }
            }
        }

        Long orgId = clusterUpdateDTO.getOrgId();
        if (!Objects.equals(orgId, clusterTbl.getClusterOrgId())) {
            needUpdate = true;
            clusterTbl.setClusterOrgId(orgId);
        }
        String adminEmails = clusterUpdateDTO.getAdminEmails();
        if (!Objects.equals(adminEmails, clusterTbl.getClusterAdminEmails())) {
            needUpdate = true;
            clusterTbl.setClusterAdminEmails(adminEmails);
        }

		if (needUpdate) {
			clusterDao.updateCluster(clusterTbl);
			return RetMessage.SUCCESS;
		} else {
			return String.format("No field changes for cluster: %s", clusterName);
		}
	}

	@Override
	@DalTransaction
	public void updateCluster(String clusterName, ClusterModel cluster) {
		ClusterTbl proto = find(clusterName);
		if (null == proto) throw new BadRequestException("Cannot find cluster");

		if (proto.getId() != cluster.getClusterTbl().getId()) {
			throw new BadRequestException("Cluster not match.");
		}
		proto.setClusterDescription(cluster.getClusterTbl().getClusterDescription());
		proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		if (!checkEmails(cluster.getClusterTbl().getClusterAdminEmails())) {
			throw new IllegalArgumentException("Emails should be ctrip emails and separated by comma or semicolon");
		}
		proto.setClusterAdminEmails(cluster.getClusterTbl().getClusterAdminEmails());
		proto.setClusterOrgId(getOrgIdFromClusterOrgName(cluster.getClusterTbl()));
		// organization info should not be updated by cluster,
		// it's automatically updated by scheduled task
		proto.setOrganizationInfo(null);

		clusterDao.updateCluster(proto);
	}

	@Override
	public void updateActivedcId(long id, long activeDcId) {

		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(id);
		clusterTbl.setActivedcId(activeDcId);

		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateActivedcId(clusterTbl, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void updateStatusById(long id, ClusterStatus clusterStatus, long migrationEventId) {

		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(id);
		clusterTbl.setStatus(clusterStatus.toString());
		clusterTbl.setMigrationEventId(migrationEventId);

		if (clusterStatus.equals(ClusterStatus.Normal)) {
			// reset migration id when exit migration
			clusterTbl.setMigrationEventId(0);
			queryHandler.handleUpdate(new DalQuery<Integer>() {
				@Override
				public Integer doQuery() throws DalException {
					return dao.updateByPK(clusterTbl, ClusterTblEntity.UPDATESET_MIGRATION_STATUS);
				}
			});
		} else {
			queryHandler.handleUpdate(new DalQuery<Integer>() {
				@Override
				public Integer doQuery() throws DalException {
					return dao.updateStatusById(clusterTbl, ClusterTblEntity.UPDATESET_MIGRATION_STATUS);
				}
			});
		}
	}

	@Override
	@Transactional
	@DalTransaction
	public void deleteCluster(String clusterName) {
		ClusterTbl cluster = this.checkCluster(clusterName);
		cluster.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());

        List<DcTbl> relatedDcs = dcService.findClusterRelatedDc(clusterName);
        ClusterEvent clusterEvent = clusterDeleteEventFactory.createClusterEvent(clusterName, cluster);

		try {
			// delete shards
			List<ShardTbl> shards = shardDao.queryAllShardsByClusterId(cluster.getId());
			if(!CollectionUtils.isEmpty(shards)) {
				shardDao.deleteShardsBatch(shards);
			}
			// delete dc cluster
			List<DcClusterTbl> dcClusters = dcClusterService.findClusterRelated(cluster.getId());
			if(!CollectionUtils.isEmpty(dcClusters)) {
				dcClusterDao.deleteDcClustersBatch(dcClusters);
			}
			//delete az group cluster
			azGroupClusterRepository.deleteByClusterId(cluster.getId());
			// delete repl direction
			List<ReplDirectionTbl> replDirections =
				replDirectionService.findAllReplDirectionTblsByCluster(cluster.getId());
			if (!CollectionUtils.isEmpty(replDirections)) {
				replDirectionService.deleteReplDirectionBatch(replDirections);
			}

			clusterDao.deleteCluster(cluster);
		} catch (Exception e) {
			throw new ServerException(e.getMessage());
		}

		if (null != clusterEvent) {
            // Call cluster delete event
			clusterEvent.onEvent();
		}
		// Notify meta server
		if (consoleConfig.shouldNotifyClusterTypes().contains(cluster.getClusterType()))
			notifier.notifyClusterDelete(clusterName, relatedDcs);
	}

	@Override
	public void bindDc(DcClusterTbl dcClusterTbl) {
		final ClusterTbl cluster = find(dcClusterTbl.getClusterName());
		final DcTbl dc = dcService.find(dcClusterTbl.getDcName());
		if (null == dc || null == cluster) throw new BadRequestException("Cannot bind dc due to unknown dc or cluster");

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				ClusterType clusterType = ClusterType.lookup(cluster.getClusterType());
				if (consoleConfig.supportSentinelHealthCheck(clusterType, dcClusterTbl.getClusterName())) {
                    AzGroupClusterEntity azGroupCluster = azGroupClusterRepository.selectByClusterIdAndAz(cluster.getId(), dc.getDcName());
                    if (azGroupCluster != null) {
                        String azGroupType = azGroupCluster.getAzGroupClusterType();
                        clusterType = StringUtil.isEmpty(azGroupType) ? clusterType : ClusterType.lookup(azGroupType);
                    }
                    SentinelGroupModel sentinelGroupModel = sentinelBalanceService.selectSentinel(dc.getDcName(), clusterType);
					return clusterDao.bindDc(cluster, dc, dcClusterTbl, sentinelGroupModel);
				} else
					return clusterDao.bindDc(cluster, dc, dcClusterTbl, null);
			}
		});
	}

    @Override
	@Transactional
	@DalTransaction
    public void bindDc(String clusterName, String dcName) {
        ClusterTbl cluster = this.checkCluster(clusterName);
        DcTbl dc = this.checkDc(dcName);
        this.checkDcClusterNotExist(cluster, dc);

        List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(cluster.getId());
        if (!CollectionUtils.isEmpty(azGroupClusters)) {
            throw new BadRequestException(String.format("Cluster: %s in Az mode, cannot bind dc", clusterName));
        }
        DcClusterEntity dcCluster = new DcClusterEntity();
        dcCluster.setClusterId(cluster.getId());
        dcCluster.setDcId(dc.getId());
        dcClusterRepository.insert(dcCluster);

        List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
        List<DcClusterShardTbl> dcClusterShards = new ArrayList<>();
        for (ShardTbl shard : shards) {
            DcClusterShardTbl dcClusterShard = new DcClusterShardTbl();
            dcClusterShard.setDcClusterId(dcCluster.getDcClusterId());
            dcClusterShard.setShardId(shard.getId());

            Long sentinelId = this.findDcClusterShardSentinelId(cluster, null, dcName, shard.getId());
            if (sentinelId != null) {
                dcClusterShard.setSetinelId(sentinelId);
            }
            dcClusterShards.add(dcClusterShard);
        }
        if (!CollectionUtils.isEmpty(dcClusterShards)) {
            dcClusterShardService.insertBatch(dcClusterShards);
        }
    }

    private Long findDcClusterShardSentinelId(ClusterTbl cluster, ClusterType azGroupType, String dcName, long shardId) {
        SentinelGroupModel sentinelGroupModel = null;
        ClusterType clusterType = ClusterType.lookup(cluster.getClusterType());
        if (consoleConfig.supportSentinelHealthCheck(clusterType, cluster.getClusterName())) {
			ClusterType type = azGroupType == null ? clusterType : azGroupType;
			sentinelGroupModel = sentinelBalanceService.selectSentinel(dcName, type);
        }

        Long sentinelId = sentinelGroupModel == null ? null : sentinelGroupModel.getSentinelGroupId();

        if (clusterType == ClusterType.CROSS_DC) {
            List<DcClusterShardTbl> existDcClusterShards = dcClusterShardService.findByShardId(shardId);
            if (!CollectionUtils.isEmpty(existDcClusterShards)) {
                sentinelId = existDcClusterShards.get(0).getSetinelId();
            }
        }

        return sentinelId;
    }

    @Override
	@Transactional
	@DalTransaction
    public void bindRegionAz(String clusterName, String regionName, String azName) {
        ClusterTbl cluster = this.checkCluster(clusterName);
        DcTbl dc = this.checkDc(azName);
        this.checkDcClusterNotExist(cluster, dc);

        List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(cluster.getId());
        if (CollectionUtils.isEmpty(azGroupClusters)) {
            throw new BadRequestException(String.format("Cluster: %s in DC mode, cannot bind az", clusterName));
        }
        for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
            AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
            if (Objects.equals(azGroup.getRegion(), regionName)) {
                // 更改az group name
                List<String> azs = azGroup.getAzsAsList();
                azs.add(azName);
                AzGroupModel newAzGroup = azGroupCache.getAzGroupByAzs(azs);
                if (newAzGroup == null) {
                    throw new BadRequestException("No Region contains such azs: " + azName);
                }

                // 更新az group cluster中对应的az group id
                azGroupClusterRepository.updateAzGroupId(azGroupCluster.getId(), newAzGroup.getId());
                // 新增dc cluster
                DcClusterEntity dcCluster = new DcClusterEntity();
                dcCluster.setClusterId(cluster.getId());
                dcCluster.setDcId(dc.getId());
                dcCluster.setAzGroupClusterId(azGroupCluster.getId());
                dcClusterRepository.insert(dcCluster);
				// 新增dc cluster shard
				ClusterType azGroupType = ClusterType.lookup(azGroupCluster.getAzGroupClusterType());
				List<DcClusterShardTbl> dcClusterShards = new ArrayList<>();
				List<ShardEntity> shards = shardRepository.selectByAzGroupClusterId(azGroupCluster.getId());
				for (ShardEntity shard : shards) {
					long shardId = shard.getId();
					DcClusterShardTbl dcClusterShard = new DcClusterShardTbl();
					dcClusterShard.setDcClusterId(dcCluster.getDcClusterId());
					dcClusterShard.setShardId(shardId);

					Long sentinelId = this.findDcClusterShardSentinelId(cluster, azGroupType, azName, shardId);
					if (sentinelId != null) {
						dcClusterShard.setSetinelId(sentinelId);
					}
					dcClusterShards.add(dcClusterShard);
				}
				if (!CollectionUtils.isEmpty(dcClusterShards)) {
					dcClusterShardService.insertBatch(dcClusterShards);
				}

                return;
            }
        }
        // 不存在对应Region，新建
        AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity();
        azGroupCluster.setClusterId(cluster.getId());
        AzGroupModel azGroup = azGroupCache.getAzGroupByAzs(Collections.singletonList(azName));
		if (azGroup == null) {
			throw new BadRequestException("No Region contains such azs: " + azName);
		}
        azGroupCluster.setAzGroupId(azGroup.getId());
        Long dcId = dcService.find(azName).getId();
        azGroupCluster.setActiveAzId(dcId);
        azGroupCluster.setAzGroupClusterType(ClusterType.SINGLE_DC.toString());
        azGroupClusterRepository.insert(azGroupCluster);

        DcClusterEntity dcCluster = new DcClusterEntity();
        dcCluster.setClusterId(cluster.getId());
        dcCluster.setDcId(dcId);
        dcCluster.setAzGroupClusterId(azGroupCluster.getId());
        dcClusterRepository.insert(dcCluster);
    }

    @Override
    @DalTransaction
	public void unbindAz(String clusterName, String azName) {
		ClusterTbl cluster = this.checkCluster(clusterName);
		DcTbl dc = this.checkDc(azName);
		this.checkDcClusterExist(cluster, dc);
		List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(cluster.getId());
        if (CollectionUtils.isEmpty(azGroupClusters)) {
            // dc模式
            this.unbindDc(cluster, dc);
        } else {
            // az模式
            for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
                AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
                if (azGroup != null && azGroup.containsAz(azName)) {
                    this.unbindRegionAz(cluster, azGroup, dc);
                    return;
                }
            }
            throw new BadRequestException(String.format("Cluster: %s not bind az: %s", clusterName, azName));
        }
	}

    @DalTransaction
    @Override
	public void unbindDc(String clusterName, String dcName) {
		final ClusterTbl cluster = find(clusterName);
		final DcTbl dc = dcService.find(dcName);
		if (null == dc || null == cluster)
			throw new BadRequestException("Cannot unbind dc due to unknown dc or cluster");

		DcClusterTbl dcClusterTbl = dcClusterService.find(dc.getId(), cluster.getId());
        ClusterType azGroupType = azGroupClusterRepository.selectAzGroupTypeById(dcClusterTbl.getAzGroupClusterId());
		if (azGroupType == ClusterType.SINGLE_DC) {
			List<ShardTbl> shardTbls = shardService.findAllShardByDcCluster(dc.getId(), cluster.getId());
			if (!CollectionUtils.isEmpty(shardTbls)) {
				List<String> shardNames = shardTbls.stream().map(ShardTbl::getShardName).collect(Collectors.toList());
				shardService.deleteShards(cluster, shardNames);
			}
			applierService.deleteAppliersByClusterAndToDc(dc.getId(), cluster.getId());
		}
		queryHandler.handleQuery(() -> clusterDao.unbindDc(cluster, dc));

		// Notify meta server
		if (consoleConfig.shouldNotifyClusterTypes().contains(cluster.getClusterType()))
			notifier.notifyClusterDelete(clusterName, Collections.singletonList(dc));
	}

    private void unbindDc(ClusterTbl cluster, DcTbl dc) {
        if (cluster.getActivedcId() == dc.getId()) {
            throw new BadRequestException("cannot unbind active dc");
        }
        queryHandler.handleQuery(() -> clusterDao.unbindDc(cluster, dc));

        if (consoleConfig.shouldNotifyClusterTypes().contains(cluster.getClusterType()))
            notifier.notifyClusterDelete(cluster.getClusterName(), Collections.singletonList(dc));
    }

    private void unbindRegionAz(ClusterTbl cluster, AzGroupModel azGroup, DcTbl az) {
        List<String> azs = azGroup.getAzsAsList();
        String azName = az.getDcName();
        azs.remove(azName);
        if (azs.isEmpty()) {
            azGroupClusterRepository.deleteByClusterIdAndAzGroupId(cluster.getId(), azGroup.getId());
            List<ShardTbl> toDeleteShards = shardService.findAllShardByDcCluster(az.getId(), cluster.getId());
            try {
                shardDao.deleteShardsBatch(toDeleteShards);
            } catch (DalException e) {
                throw new ServerException(e.getMessage());
            }
        } else {
            AzGroupModel newAzGroup = azGroupCache.getAzGroupByAzs(azs);
            if (newAzGroup == null) {
				throw new BadRequestException(
					String.format("Cannot unbind az: %s from Region: %s, rest azs not a region",
						azName, azGroup.getRegion()));
            }
            AzGroupClusterEntity azGroupCluster =
                azGroupClusterRepository.selectByClusterIdAndAzGroupId(cluster.getId(), azGroup.getId());
            if (azGroupCluster.getActiveAzId() == az.getId()) {
                throw new BadRequestException("not allow unbind active az");
            }

            azGroupClusterRepository.updateAzGroupId(azGroupCluster.getId(), newAzGroup.getId());
        }

        queryHandler.handleQuery(() -> clusterDao.unbindDc(cluster, az));
        if (consoleConfig.shouldNotifyClusterTypes().contains(cluster.getClusterType()))
            notifier.notifyClusterDelete(cluster.getClusterName(), Collections.singletonList(az));
    }

	@Override
	public void update(final ClusterTbl cluster) {
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(cluster, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

    @Override
    public void upgradeAzGroup(String clusterName) {
        ClusterTbl cluster = clusterDao.findClusterByClusterName(clusterName);
        if (cluster == null) {
            throw new BadRequestException(String.format("Cluster: %s not exist", clusterName));
        }
        long cnt = azGroupClusterRepository.countByClusterId(cluster.getId());
        if (cnt > 0L) {
			logger.warn("[upgradeAzGroup]cluster: {} already in AzGroup mode", clusterName);
			return;
        }
        ClusterType clusterType = ClusterType.lookup(cluster.getClusterType());
        if (clusterType != ClusterType.ONE_WAY && clusterType != ClusterType.BI_DIRECTION) {
            throw new BadRequestException("Only ONE_WAY/BI_DIRECTION cluster can upgrade to az group");
        }
        Map<Long, String> dcIdNameMap = dcService.dcNameMap();
        List<DcClusterEntity> dcClusters = dcClusterRepository.selectByClusterId(cluster.getId());
        if (clusterType == ClusterType.ONE_WAY) {
            AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity();
            azGroupCluster.setClusterId(cluster.getId());
            azGroupCluster.setActiveAzId(cluster.getActivedcId());
            azGroupCluster.setAzGroupClusterType(ClusterType.ONE_WAY.toString());

            List<String> dcs = dcClusters.stream()
                .map(dcCluster -> dcIdNameMap.get(dcCluster.getDcId()))
                .collect(Collectors.toList());
            AzGroupModel azGroup = azGroupCache.getAzGroupByAzs(dcs);
            if (azGroup == null) {
                throw new BadRequestException(
                    String.format("Cluster: %s contains multi region, cannot upgrade", clusterName));
            }
            azGroupCluster.setAzGroupId(azGroup.getId());

            azGroupClusterRepository.insert(azGroupCluster);

            List<Long> dcClusterIds = dcClusters.stream()
                .map(DcClusterEntity::getDcClusterId)
                .collect(Collectors.toList());
            dcClusterRepository.batchUpdateAzGroupClusterId(dcClusterIds, azGroupCluster.getId());
        }
        if (clusterType == ClusterType.BI_DIRECTION) {
            for (DcClusterEntity dcCluster : dcClusters) {
                AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity();
                azGroupCluster.setClusterId(cluster.getId());
                String dc = dcIdNameMap.get(dcCluster.getDcId());
                AzGroupModel azGroup = azGroupCache.getAzGroupByAzs(Collections.singletonList(dc));
				if (azGroup == null) {
					throw new BadRequestException(
						String.format("Cluster: %s contains %s, which can't upgrade to az group", clusterName, dc));
				}
                azGroupCluster.setAzGroupId(azGroup.getId());
                azGroupCluster.setActiveAzId(dcCluster.getDcId());

                azGroupClusterRepository.insert(azGroupCluster);
                dcClusterRepository.updateAzGroupClusterId(dcCluster.getDcClusterId(), azGroupCluster.getId());
            }
        }
    }

    @Override
	@DalTransaction
	public void exchangeName(Long formerClusterId, String formerClusterName, Long latterClusterId, String latterClusterName) {
		logger.info("[exchangeName]{}:{} <-> {}:{}", formerClusterName, formerClusterId, latterClusterName, latterClusterId);
		ClusterTbl former = queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findClusterByClusterName(formerClusterName, ClusterTblEntity.READSET_FULL);
			}
		});
		ClusterTbl latter = queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findClusterByClusterName(latterClusterName, ClusterTblEntity.READSET_FULL);
			}
		});

		if (former == null) throw new BadRequestException("former cluster not found");
		if (latter == null) throw new BadRequestException("latter cluster not found");
		if (former.getId() != formerClusterId) throw new BadRequestException("former cluster name Id not match");
		if (latter.getId() != latterClusterId) throw new BadRequestException("latter cluster name Id not match");

		String tmpClusterName = UUID.randomUUID().toString();
		former.setClusterName(tmpClusterName);
		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(former, ClusterTblEntity.UPDATESET_FULL);
			}
		});

		latter.setClusterName(formerClusterName);
		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(latter, ClusterTblEntity.UPDATESET_FULL);
			}
		});

		former.setClusterName(latterClusterName);
		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(former, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

    // 检测cluster是否存在，不存在则抛出异常
    private ClusterTbl checkCluster(String clusterName) {
        ClusterTbl cluster = clusterDao.findClusterByClusterName(clusterName);
        if (cluster == null) {
            throw new BadRequestException("Cannot find cluster: " + clusterName);
        }
        return cluster;
    }

    // 检测az是否存在，不存在则抛出异常
    private DcTbl checkDc(String dcName) {
        DcTbl dc = dcService.find(dcName);
        if (dc == null) {
            throw new BadRequestException("Cannot find dc: " + dcName);
        }
        return dc;
    }

    // 检测dc cluster是否存在，不存在则抛异常
    private void checkDcClusterExist(ClusterTbl cluster, DcTbl dc) {
        DcClusterEntity dcCluster = dcClusterRepository.selectByClusterIdAndDcId(cluster.getId(), dc.getId());
        if (dcCluster == null) {
            throw new BadRequestException(
                String.format("Cluster: %s not exist %s dc", cluster.getClusterName(), dc.getDcName()));
        }
    }

    // 检测dc cluster是否不存在，存在则抛异常
    private void checkDcClusterNotExist(ClusterTbl cluster, DcTbl dc) {
        DcClusterEntity dcCluster = dcClusterRepository.selectByClusterIdAndDcId(cluster.getId(), dc.getId());
        if (dcCluster != null) {
            throw new BadRequestException(
                String.format("Cluster: %s already exist %s dc", cluster.getClusterName(), dc.getDcName()));
        }
    }

	public boolean checkEmails(String emails) {
		if (emails == null || emails.trim().isEmpty()) {
			return false;
		}
		String splitter = "\\s*(,|;)\\s*";
		String[] emailStrs = StringUtil.splitRemoveEmpty(splitter, emails);
		for (String email : emailStrs) {
			EmailService.CheckEmailResponse response = EmailService.DEFAULT.checkEmailAddress(email);
			if (!response.isOk())
				return false;
		}
		return true;
	}

	@Override
	public Set<String> findMigratingClusterNames() {
		List<ClusterTbl> clusterTbls = clusterDao.findMigratingClusterNames();
		return clusterTbls.stream().map(ClusterTbl::getClusterName).collect(Collectors.toSet());
	}

	@Override
	public List<ClusterTbl> findErrorMigratingClusters() {
		List<ClusterTbl> errorClusters = Lists.newArrayList();
		List<ClusterTbl> clustersWithEvents = clusterDao.findMigratingClustersWithEvents();
		for (ClusterTbl clusterWithEvent : clustersWithEvents) {
			if (clusterWithEvent.getMigrationEvent().getId() == 0) {
				errorClusters.add(clusterWithEvent);
			}
		}
		List<ClusterTbl> overviews = clusterDao.findMigratingClustersOverview();
		for (ClusterTbl overview : overviews) {
			MigrationClusterTbl migrationClusterTbl = overview.getMigrationClusters();
			String migrationStatus = migrationClusterTbl.getStatus();
			if (migrationStatus != null) {
				String clusterStatus = overview.getStatus();
				if (MigrationStatus.valueOf(migrationStatus).getClusterStatus().toString().equals(clusterStatus)) {
					continue;
				}
			}
			overview.setMigrationEvent(null);
			overview.setMigrationClusters(null);
			errorClusters.add(overview);
		}
		return errorClusters;
	}

	@Override
	public List<ClusterTbl> findMigratingClusters() {
		return clusterDao.findMigratingClustersOverview();
	}

	@Override
	public List<ClusterListUnhealthyClusterModel> findUnhealthyClusters() {
		try {
			XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
			if (xpipeMeta == null || xpipeMeta.getDcs() == null) {
				return Collections.emptyList();
			}

			Map<String, ClusterListUnhealthyClusterModel> result = Maps.newHashMap();
			UnhealthyInfoModel unhealthyInfo = delayService.getAllUnhealthyInstance();
			UnhealthyInfoModel parallelUnhealthyInfo = delayService.getAllUnhealthyInstanceFromParallelService();
			if (null != parallelUnhealthyInfo) unhealthyInfo.merge(parallelUnhealthyInfo);

			for (String unhealthyCluster : unhealthyInfo.getUnhealthyClusterNames()) {
				ClusterListUnhealthyClusterModel cluster = new ClusterListUnhealthyClusterModel(unhealthyCluster);
				cluster.setMessages(unhealthyInfo.getUnhealthyClusterDesc(unhealthyCluster));
				cluster.setUnhealthyShardsCnt(unhealthyInfo.countUnhealthyShardByCluster(unhealthyCluster));
				cluster.setUnhealthyRedisCnt(unhealthyInfo.countUnhealthyRedisByCluster(unhealthyCluster));
				result.put(unhealthyCluster, cluster);
			}

			return richClusterInfo(result);
		} catch (Exception e) {
			logger.error("[findUnhealthyClusters]", e);
			return Collections.emptyList();
		}
	}

	@VisibleForTesting
	protected List<ClusterListUnhealthyClusterModel> richClusterInfo(Map<String, ClusterListUnhealthyClusterModel> clusters) {

		if (clusters.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> clusterNames = Lists.newArrayListWithExpectedSize(clusters.size());
		clusterNames.addAll(clusters.keySet());
		List<ClusterTbl> clusterTbls = clusterDao.findClustersWithName(clusterNames);

		List<ClusterListUnhealthyClusterModel> result = Lists.newArrayListWithExpectedSize(clusterTbls.size());

		for (ClusterTbl clusterTbl : clusterTbls) {
			ClusterListUnhealthyClusterModel cluster = clusters.get(clusterTbl.getClusterName());
			cluster.setActivedcId(clusterTbl.getActivedcId())
					.setClusterAdminEmails(clusterTbl.getClusterAdminEmails())
					.setClusterDescription(clusterTbl.getClusterDescription())
					.setClusterType(clusterTbl.getClusterType());
			result.add(cluster);
		}
		return result;
	}

	@Override
	public List<ClusterTbl> findAllClusterByDcNameBind(String dcName) {
		if (StringUtil.isEmpty(dcName))
			return Collections.emptyList();

		long dcId = dcService.find(dcName).getId();

		List<ClusterTbl> result = queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findClustersBindedByDcId(dcId, ClusterTblEntity.READSET_FULL_WITH_ORG);
			}
		});

		result = fillClusterOrgName(result);
		return setOrgNullIfNoOrgIdExsits(result);
	}

	@Override
	public List<ClusterTbl> findAllClusterByDcNameBindAndType(String dcName, String clusterType) {
		List<ClusterTbl> dcClusters = findAllClusterByDcNameBind(dcName);
		if (clusterType.isEmpty()) return dcClusters;
		return dcClusters.stream().filter(clusterTbl -> clusterTbl.getClusterType().equalsIgnoreCase(clusterType)).collect(Collectors.toList());
	}

	@Override
	public List<ClusterTbl> findActiveClustersByDcName(String dcName) {
		if (StringUtil.isEmpty(dcName))
			return Collections.emptyList();

		return findClustersWithOrgInfoByActiveDcId(dcService.find(dcName).getId());
	}

	@Override
	public List<ClusterTbl> findActiveClustersByDcNameAndType(String dcName, String clusterType) {
		List<ClusterTbl> dcActiveClusters = findActiveClustersByDcName(dcName);
		if (clusterType.isEmpty()) return dcActiveClusters;
		return dcActiveClusters.stream().filter(clusterTbl -> clusterTbl.getClusterType().equalsIgnoreCase(clusterType)).collect(Collectors.toList());
	}

	@Override
	public List<ClusterTbl> findAllClustersByDcName(String dcName) {
		if (StringUtil.isEmpty(dcName))
			return Collections.emptyList();

		return clusterDao.findAllByDcId(dcService.find(dcName).getId());
	}

	@Override
	public List<ClusterTbl> findAllClusterByKeeperContainer(long keeperContainerId) {
		List<Long> clusterIds = redisService.findClusterIdsByKeeperContainer(keeperContainerId);
		if (clusterIds.isEmpty()) return Collections.emptyList();
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findClustersWithOrgInfoById(clusterIds, ClusterTblEntity.READSET_FULL_WITH_ORG);
			}
		});
	}

	@Override
	public List<Set<String>> divideClusters(int partsCnt) {
		List<ClusterTbl> allClusters = queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_NAME);
			}
		});

		if (null == allClusters) return Collections.emptyList();

		List<Set<String>> parts = new ArrayList<>(partsCnt);
		IntStream.range(0, partsCnt).forEach(i -> parts.add(new HashSet<>()));

		allClusters.forEach(clusterTbl -> parts.get((int) (clusterTbl.getId() % partsCnt)).add(clusterTbl.getClusterName()));
		return parts;
	}

	@Override
	public List<RouteInfoModel> findClusterDefaultRoutesBySrcDcNameAndClusterName(String srcDcName, String clusterName) {
		List<RouteInfoModel> defaultRoutes = new ArrayList<>();

		ClusterTbl clusterTbl = find(clusterName);
		if (null == clusterTbl) throw new BadRequestException("not exist cluster " + clusterName);
		if (!needCheckClusterRouteInfo(clusterTbl.getClusterType())) return defaultRoutes;

		List<String> dstDcNames = parseDstDcs(clusterTbl);
		Map<String, RouteMeta> chooseRoutes = metaCache.chooseDefaultRoutes(clusterName, srcDcName, dstDcNames, (int) clusterTbl.getClusterOrgId());
		if (chooseRoutes == null || chooseRoutes.isEmpty()) return defaultRoutes;

		Map<Long, RouteInfoModel> routeIdInfoModelMap = routeService.getRouteIdInfoModelMap();
		for (RouteMeta routeMeta : chooseRoutes.values()) {
			defaultRoutes.add(routeIdInfoModelMap.get(routeMeta.getId()));
		}
		Collections.sort(defaultRoutes);
		logger.debug("cluster {} exists default routes {} at source dc {}", clusterName, defaultRoutes, srcDcName);

		return defaultRoutes;
	}

	@VisibleForTesting
	protected List<String> parseDstDcs(ClusterTbl clusterTbl) {
		DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);

		List<String> dstDcs = new ArrayList<>();
		if (ClusterType.lookup(clusterTbl.getClusterType()).supportMultiActiveDC()) {
			List<DcClusterTbl> clusterRelated = dcClusterService.findClusterRelated(clusterTbl.getId());
			if (clusterRelated == null || clusterRelated.isEmpty()) return dstDcs;

			clusterRelated.forEach((dcClusterTbl -> dstDcs.add(mapper.getName(dcClusterTbl.getDcId()))));
		} else {
			dstDcs.add(mapper.getName(clusterTbl.getActivedcId()));
		}
		return dstDcs;
	}

	@Override
	public List<RouteInfoModel> findClusterUsedRoutesBySrcDcNameAndClusterName(String srcDcName, String clusterName) {
		ClusterTbl clusterTbl = find(clusterName);
		if (!needCheckClusterRouteInfo(clusterTbl.getClusterType())) return Collections.emptyList();

		Map<String, List<ProxyChain>> proxyChains = proxyService.getProxyChains(srcDcName, clusterName);

		List<RouteInfoModel> allRoutes = routeService.getAllActiveRouteInfoModelsByTagAndSrcDcName(Route.TAG_META, srcDcName);

		Set<RouteInfoModel> result = new HashSet<>();
		for (Map.Entry<String, List<ProxyChain>> shardChains : proxyChains.entrySet()) {
			List<ProxyChain> peerChains = shardChains.getValue();
			for (ProxyChain chain : peerChains) {
				result.add(getRouteInfoModelFromProxyChainModel(allRoutes, new ProxyChainModel(chain, chain.getPeerDcId(), srcDcName)));
			}
		}

		List<RouteInfoModel> usedRoutes = Lists.newArrayList(result);
		Collections.sort(usedRoutes);
		logger.debug("cluster {} exists used routes {} at source dc {}", clusterName, usedRoutes, srcDcName);

		return usedRoutes;
	}

	@VisibleForTesting
	protected RouteInfoModel getRouteInfoModelFromProxyChainModel(List<RouteInfoModel> allDcRoutes, ProxyChainModel proxyChainModel) {
		String srcProxy = getSrcProxy(proxyChainModel);
		String dstProxy = getDstProxy(proxyChainModel);

		for (RouteInfoModel route : allDcRoutes) {
			if (route.getSrcProxies().contains(srcProxy) && route.getDstProxies().contains(dstProxy))
				return route;
		}
		return null;
	}

	private String getSrcProxy(ProxyChainModel proxyChainModel) {
		logger.debug("[getSrcProxy] get src proxy from tunnel:{}", proxyChainModel.getBackupDcTunnel());
		String[] ips = StringUtil.splitRemoveEmpty(PROXY_SPLITTER, proxyChainModel.getBackupDcTunnel().getTunnelId());
		String[] results = ips[2].substring(2).split(":");

		return String.format(PROXYTCP, results[0]);
	}

	private String getDstProxy(ProxyChainModel proxyChainModel) {
		String[] ips = StringUtil.splitRemoveEmpty(PROXY_SPLITTER, proxyChainModel.getBackupDcTunnel().getTunnelId());
		if (proxyChainModel.getOptionalTunnel() != null)
			ips = StringUtil.splitRemoveEmpty(PROXY_SPLITTER, proxyChainModel.getOptionalTunnel().getTunnelId());

		logger.debug("[getDstProxy] get dst proxy from tunnel:{}", proxyChainModel.getOptionalTunnel() == null ?
				proxyChainModel.getBackupDcTunnel() : proxyChainModel.getOptionalTunnel());
		String[] results = ips[3].substring(3).split(":");
		return String.format(PROXYTLS, results[0]);
	}

	@Override
	public List<RouteInfoModel> findClusterDesignateRoutesBySrcDcNameAndClusterName(String srcDcName, String clusterName) {
		ClusterTbl clusterTbl = find(clusterName);
		if (null == clusterTbl) throw new BadRequestException("not exist cluster " + clusterName);
		if (!needCheckClusterRouteInfo(clusterTbl.getClusterType())) return Collections.emptyList();

		String clusterDesignatedRouteIds = clusterTbl.getClusterDesignatedRouteIds();
		if (StringUtil.isEmpty(clusterDesignatedRouteIds)) return Collections.emptyList();

		Map<Long, RouteInfoModel> routeIdInfoModelMap = routeService.getRouteIdInfoModelMap();
		List<RouteInfoModel> designatedRoutes = new ArrayList<>();
		Set<String> routeIds = Sets.newHashSet(clusterDesignatedRouteIds.split(DESIGNATED_ROUTE_ID_SPLITTER));
		routeIds.forEach(routeId -> {
			RouteInfoModel routeInfoModel = routeIdInfoModelMap.get(Long.valueOf(routeId));
			if (routeInfoModel != null && srcDcName.equalsIgnoreCase(routeInfoModel.getSrcDcName())) {
				designatedRoutes.add(routeInfoModel);
			}
		});
		Collections.sort(designatedRoutes);
		logger.debug("cluster {} exists designated routes {} at source dc {}", clusterName, designatedRoutes, srcDcName);

		return designatedRoutes;
	}

	@Override
	public void updateClusterDesignateRoutes(String clusterName, String srcDcName, List<RouteInfoModel> newDesignatedRoutes) {
		ClusterTbl clusterTbl = find(clusterName);
		if (null == clusterTbl) throw new BadRequestException("not exist cluster " + clusterName);

		String oldClusterDesignatedRouteIds = clusterTbl.getClusterDesignatedRouteIds();
		Set<String> newDesignatedRouteIds = new HashSet<>();

		Map<Long, RouteInfoModel> allRoutesMap = new HashMap<>();
		routeService.getAllActiveRouteInfoModels().forEach((routeInfoModel -> {
			allRoutesMap.put(routeInfoModel.getId(), routeInfoModel);
		}));

		if (!StringUtil.isEmpty(oldClusterDesignatedRouteIds)) {
			Set<String> oldClusterDesignatedRoutes = Sets.newHashSet(oldClusterDesignatedRouteIds.split(DESIGNATED_ROUTE_ID_SPLITTER));
			for (String routeId : oldClusterDesignatedRoutes) {
				RouteInfoModel routeInfoModel = allRoutesMap.get(Long.parseLong(routeId));
				if (routeInfoModel == null) continue;
				if (srcDcName.equalsIgnoreCase(routeInfoModel.getSrcDcName())) continue;

				newDesignatedRouteIds.add(routeId);
			}
		}

		newDesignatedRoutes.forEach(route -> newDesignatedRouteIds.add(String.valueOf(route.getId())));

		logger.debug("change designated routes of cluster {} from {} to {} at source dc {}",
				clusterName, oldClusterDesignatedRouteIds, newDesignatedRouteIds, srcDcName);

		updateClusterDesignatedRouteIds(clusterTbl.getId(), StringUtil.join(",", arg -> arg, newDesignatedRouteIds));

		ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
		if (consoleConfig.shouldNotifyClusterTypes().contains(clusterType.name())) {
			notifier.notifyClusterUpdate(clusterName, Collections.singletonList(srcDcName));
		}
	}

	private void updateClusterDesignatedRouteIds(long clusterId, String newClusterDesignateRoutes) {
		ClusterTbl proto = new ClusterTbl();
		proto.setId(clusterId);
		proto.setClusterDesignatedRouteIds(newClusterDesignateRoutes);

		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateClusterDesignatedRouteIds(proto, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

	public UnexpectedRouteUsageInfoModel findUnexpectedRouteUsageInfoModel() {
		UnexpectedRouteUsageInfoModel unexpectedRouteUsageInfoModel = new UnexpectedRouteUsageInfoModel();

		XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
		if (xpipeMeta == null || xpipeMeta.getDcs() == null) {
			return unexpectedRouteUsageInfoModel;
		}

		for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
			for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
				if (!needCheckClusterRouteInfo(clusterMeta.getType())) continue;

				checkClusterRouteUsageInfo(clusterMeta, dcMeta.getId(), dcMeta.getRoutes(), unexpectedRouteUsageInfoModel);
			}
		}
		return unexpectedRouteUsageInfoModel;
	}

	private boolean needCheckClusterRouteInfo(String clusterType) {
		return ClusterType.isSameClusterType(clusterType, ClusterType.BI_DIRECTION)
				|| ClusterType.isSameClusterType(clusterType, ClusterType.ONE_WAY);
	}

	private void checkClusterRouteUsageInfo(ClusterMeta clusterMeta, String srcDcName, List<RouteMeta> allDcRoutes,
											UnexpectedRouteUsageInfoModel unexpectedRouteUsageInfoModel) {
		Map<String, RouteMeta> chooseDcRouteIds = getChooseRoutes(clusterMeta, srcDcName);
		Map<String, Set<Long>> usedDcRouteIds = getUsedRoutes(srcDcName, clusterMeta, allDcRoutes);

		Set<String> allDcNames = Sets.newHashSet();
		allDcNames.addAll(chooseDcRouteIds.keySet());
		allDcNames.addAll(usedDcRouteIds.keySet());

		for (String dstDcName : allDcNames) {
			if (isUsingUnexpectedRoute(usedDcRouteIds.get(dstDcName), chooseDcRouteIds.get(dstDcName).getId())) {
				unexpectedRouteUsageInfoModel.addUsedWrongRouteCluster(clusterMeta.getId(), srcDcName, dstDcName,
						usedDcRouteIds.get(dstDcName), chooseDcRouteIds.get(dstDcName).getId());
			}
		}
	}

	private boolean isUsingUnexpectedRoute(Set<Long> usedRouteIds, Long expectedRouteId) {
		if (usedRouteIds == null && expectedRouteId == null) return false;
		if (usedRouteIds == null || expectedRouteId == null) return true;

		return !ObjectUtils.equals(usedRouteIds, Sets.newHashSet(expectedRouteId));
	}

	private Map<String, Set<Long>> getUsedRoutes(String srcDcName, ClusterMeta clusterMeta, List<RouteMeta> allDcRoutes) {
		Map<String, Set<Long>> usedDcRoutes = new HashMap<>();
		List<String> dstDcNames = parseDstDcs(clusterMeta);

		for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
			for (String dstDcName : dstDcNames) {
				if (dstDcName.equals(srcDcName)) continue;

				RouteMeta usedRoute = getShardUsedRouteByDirection(srcDcName, dstDcName, clusterMeta.getId(), shardMeta.getId(), allDcRoutes);
				if (usedRoute != null)
					MapUtils.getOrCreate(usedDcRoutes, usedRoute.getDstDc().toLowerCase(), () -> Sets.newHashSet()).add(usedRoute.getId());
			}
		}

		return usedDcRoutes;
	}

	private RouteMeta getShardUsedRouteByDirection(String srcDc, String dstDc, String clusterName, String shardName, List<RouteMeta> allDcRoutes) {
		ProxyChain chain = proxyService.getProxyChain(srcDc, clusterName, shardName, dstDc);
		return chain == null ? null : getRouteMetaFromProxyChainModel(allDcRoutes, new ProxyChainModel(chain, chain.getPeerDcId(), srcDc));
	}

	private RouteMeta getRouteMetaFromProxyChainModel(List<RouteMeta> allDcRoutes, ProxyChainModel proxyChainModel) {
		String srcProxy = getSrcProxy(proxyChainModel);
		String dstProxy = getDstProxy(proxyChainModel);

		for (RouteMeta route : allDcRoutes) {
			String[] allProxyInfos = route.getRouteInfo().split("\\h");
			Set<String> srcProxies = Sets.newHashSet(allProxyInfos[0].split(DESIGNATED_ROUTE_ID_SPLITTER));
			Set<String> dstProxies = Sets.newHashSet(allProxyInfos[allProxyInfos.length - 1].split(DESIGNATED_ROUTE_ID_SPLITTER));

			if (srcProxies.contains(srcProxy) && dstProxies.contains(dstProxy))
				return route;
		}
		return null;
	}

	private Map<String, RouteMeta> getChooseRoutes(ClusterMeta clusterMeta, String srcDcName) {
		List<String> dstDcNames = parseDstDcs(clusterMeta);
		return metaCache.chooseRoutes(clusterMeta.getId(), srcDcName, dstDcNames, clusterMeta.getOrgId());
	}

	private List<String> parseDstDcs(ClusterMeta clusterMeta) {
		if (ClusterType.lookup(clusterMeta.getType()).supportMultiActiveDC()) {
			return Lists.newArrayList(clusterMeta.getDcs().split(DESIGNATED_ROUTE_ID_SPLITTER));
		} else {
			return Lists.newArrayList(clusterMeta.getActiveDc());
		}
	}

	@Override
	public void completeReplicationByClusterAndReplDirection(ClusterTbl cluster, ReplDirectionInfoModel replDirection) {
        DcClusterTbl dcCluster = dcClusterService.find(replDirection.getToDcName(), cluster.getClusterName());
        ClusterType azGroupType = azGroupClusterRepository.selectAzGroupTypeById(dcCluster.getAzGroupClusterId());
        if (azGroupType != ClusterType.SINGLE_DC) {
			addKeepersWhenToDcIsDrMasterType(cluster, replDirection);
		} else if (ObjectUtils.equals(replDirection.getSrcDcName(), replDirection.getFromDcName())) {
			addKeepersAndAppliersWithNoTransitDc(cluster, replDirection);
		} else {
			addKeepersAndAppliersWithTransitDc(cluster, replDirection);
		}
	}

	private void addKeepersAndAppliersWithNoTransitDc(ClusterTbl cluster, ReplDirectionInfoModel replDirection) {
        DcClusterTbl dcCluster = dcClusterService.find(replDirection.getSrcDcName(), cluster.getClusterName());
        ClusterType azGroupType = azGroupClusterRepository.selectAzGroupTypeById(dcCluster.getAzGroupClusterId());
        if (azGroupType == ClusterType.SINGLE_DC) {
			doAddShardKeepersByDcAndCluster(replDirection.getSrcDcName(), cluster.getClusterName(), replDirection);
		}
		doAddSourceAppliersByDcAndCluster(replDirection.getToDcName(), cluster.getClusterName(), replDirection);
	}

	private void addKeepersWhenToDcIsDrMasterType(ClusterTbl cluster, ReplDirectionInfoModel replDirection) {
		String msg = String.format("to dc %s is Dr master Dc in cluster %s", replDirection.getToDcName(), cluster.getClusterName());
		logger.warn("[completeReplicationByReplDirection] {}", msg);
		throw new BadRequestException(msg);
	}

	private void addKeepersAndAppliersWithTransitDc(ClusterTbl cluster, ReplDirectionInfoModel replDirection) {
        DcClusterTbl dcCluster = dcClusterService.find(replDirection.getFromDcName(), cluster.getClusterName());
        ClusterType azGroupType = azGroupClusterRepository.selectAzGroupTypeById(dcCluster.getAzGroupClusterId());
        if (azGroupType != ClusterType.SINGLE_DC) {
			throw new BadRequestException(String.format("transit dc %s in cluster %s is not MASTER type", replDirection.getFromDcName(), cluster.getClusterName()));
		}

		ReplDirectionInfoModel upstreamReplDirection = replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc(cluster.getClusterName(), replDirection.getSrcDcName(), replDirection.getFromDcName());
		addUpstreamSourceKeepers(replDirection, upstreamReplDirection);

		doAddSourceKeepersByDcAndCluster(replDirection.getFromDcName(), cluster.getClusterName(), replDirection);
		doAddSourceAppliersByDcAndCluster(replDirection.getToDcName(), cluster.getClusterName(), replDirection);
	}

	private void addUpstreamSourceKeepers(ReplDirectionInfoModel replDirection, ReplDirectionInfoModel upstreamReplDirection) {
		if (upstreamReplDirection == null || upstreamReplDirection.getSrcDcName().equalsIgnoreCase(upstreamReplDirection.getFromDcName())) {
			return ;
		}
		addUpstreamSourceKeepers(upstreamReplDirection, replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc(
				upstreamReplDirection.getClusterName(), upstreamReplDirection.getSrcDcName(), upstreamReplDirection.getFromDcName()));
		doAddSourceKeepersByDcAndCluster(upstreamReplDirection.getFromDcName(), upstreamReplDirection.getClusterName(), upstreamReplDirection);
	}

	private void doAddSourceAppliersByDcAndCluster(String dcName, String clusterName, ReplDirectionInfoModel replDirection) {
		List<DcClusterShardTbl> dcClusterShards = dcClusterShardService.findAllByDcCluster(dcName, clusterName);
		if (dcClusterShards == null || dcClusterShards.isEmpty()) return;

		SourceModel sourceModel = sourceModelService.getAllSourceModelsByClusterAndReplDirection(dcName, clusterName, replDirection);
		if (sourceModel == null || sourceModel.getShards() == null || sourceModel.getShards().isEmpty()) {
			throw new BadRequestException(String.format("cluster:%s  does not exist or has no source shard in dc:%s", clusterName, dcName));
		}

		for (ShardModel shardModel : sourceModel.getShards()) {
			if (shardModel.getAppliers() != null && shardModel.getAppliers().size() != 0) {
				continue;
			}

			List<ApplierBasicInfo> bestAppliers = applierService.findBestAppliers(dcName, APPLIER_PORT_DEFAULT, (ip, port) -> true, clusterName);
			List<ApplierTbl> newAppliers = new LinkedList<>();
			bestAppliers.forEach(bestApplier -> {
				newAppliers.add(new ApplierTbl().setContainerId(bestApplier.getAppliercontainerId())
						.setIp(bestApplier.getHost()).setPort(bestApplier.getPort()));
			});
			shardModel.setAppliers(newAppliers);
			try {
				logger.info("[Update source appliers][construct]{},{},{},{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModel);
				applierService.updateAppliersAndKeepers(dcName, clusterName, shardModel.getShardTbl().getShardName(), shardModel, replDirection.getId());
				logger.info("[Update source appliers][success]{},{},{},{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModel);
			} catch (Throwable th) {
				logger.error("[Update source appliers][failed]{},{},{},{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModel, th);
				throw th;
			}
		}
	}

	private void doAddSourceKeepersByDcAndCluster(String dcName, String clusterName, ReplDirectionInfoModel replDirection) {
		SourceModel sourceModel = sourceModelService.getAllSourceModelsByClusterAndReplDirection(dcName, clusterName, replDirection);
		if (sourceModel == null || sourceModel.getShards() == null || sourceModel.getShards().isEmpty()) {
			throw new BadRequestException(String.format("cluster:%s  does not exist or has no source shard in dc:%s", clusterName, dcName));
		}
		doAddKeepersForShards(dcName, clusterName, sourceModel.getShards(), replDirection, true);

	}

	private void doAddShardKeepersByDcAndCluster(String dcName, String clusterName, ReplDirectionInfoModel replDirection) {
		List<ShardModel> shards = shardModelService.getAllShardModel(dcName, clusterName);
		if (shards == null || shards.isEmpty()) {
			throw new BadRequestException(String.format("cluster:%s  does not exist or has no shard in dc:%s", clusterName, dcName));
		}
		doAddKeepersForShards(dcName, clusterName, shards, replDirection, false);
	}

	private void doAddKeepersForShards(String dcName, String clusterName, List<ShardModel> shardModels, ReplDirectionInfoModel replDirection, boolean isSource) {
		final Map<String, Long> dcNameIdMap = dcService.dcNameIdMap();
		for (ShardModel shardModel : shardModels) {
			if (shardModel.getKeepers() != null && shardModel.getKeepers().size() != 0) {
				continue;
			}
			List<KeeperBasicInfo> newKeeperBasicInfos
					= keeperAdvancedService.findBestKeepers(dcName, KEEPER_PORT_DEFAULT, (ip, port) -> true, clusterName);
			List<RedisTbl> newKeepers = new ArrayList<>();
			newKeeperBasicInfos.forEach(keeperBasicInfo -> newKeepers.add(new RedisTbl()
					.setKeepercontainerId(keeperBasicInfo.getKeeperContainerId())
					.setRedisIp(keeperBasicInfo.getHost()).setRedisPort(keeperBasicInfo.getPort())));
			shardModel.setKeepers(newKeepers);

			try {
				logger.info("[Update keepers][construct]{},{},{},{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModel);
				if (isSource) {
					redisService.updateSourceKeepers(replDirection.getSrcDcName(), clusterName, shardModel.getShardTbl().getShardName(), dcNameIdMap.get(dcName), shardModel);
				} else {
					redisService.updateRedises(dcName, clusterName, shardModel.getShardTbl().getShardName(), shardModel);
				}
				logger.info("[Update keepers][success]{},{},{},{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModel);
			} catch (Exception e) {
				logger.error("[Update keepers][failed]{},{},{},{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModel, e);
				throw e;
			}
		}
	}

	@VisibleForTesting
	public void setClusterDeleteEventFactory(ClusterDeleteEventFactory clusterDeleteEventFactory) {
		this.clusterDeleteEventFactory = clusterDeleteEventFactory;
	}

	@VisibleForTesting
	public void setConsoleConfig(ConsoleConfig consoleConfig) {
		this.consoleConfig = consoleConfig;
	}
}
