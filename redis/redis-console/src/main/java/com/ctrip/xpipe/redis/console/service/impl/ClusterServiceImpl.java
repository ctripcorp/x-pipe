package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayAction;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayService;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListClusterModel;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterDeleteEventFactory;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterEvent;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ClusterServiceImpl extends AbstractConsoleService<ClusterTblDao> implements ClusterService {

	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterDao clusterDao;
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
	private MetaCache metaCache;

	private Random random = new Random();

	@Autowired
	private SentinelService sentinelService;

	@Autowired
	private ConsoleConfig consoleConfig;

	@Autowired
	private ClusterDeleteEventFactory clusterDeleteEventFactory;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Autowired
	private DcClusterService dcClusterService;

	@Override
	public ClusterTbl find(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
			}
    		
    	});
	}

	@Override
	public ClusterStatus clusterStatus(String clusterName) {

		ClusterTbl clusterTbl = find(clusterName);
		if(clusterTbl == null){
			throw new IllegalArgumentException("cluster not found:" + clusterName);
		}
		return ClusterStatus.valueOf(clusterTbl.getStatus());
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

		clusterTbls.forEach( clusterTbl -> clusterNames.add(clusterTbl.getClusterName()));

		return clusterNames;
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
	@DalTransaction
	public synchronized ClusterTbl createCluster(ClusterModel clusterModel) {
		ClusterTbl cluster = clusterModel.getClusterTbl();
    	List<DcTbl> slaveDcs = clusterModel.getSlaveDcs();
    	List<ShardModel> shards = clusterModel.getShards();

    	// ensure active dc assigned
    	if(XPipeConsoleConstant.NO_ACTIVE_DC_TAG == cluster.getActivedcId()) {
    		throw new BadRequestException("No active dc assigned.");
    	}
    	ClusterTbl proto = dao.createLocal();
    	proto.setClusterName(cluster.getClusterName().trim());
    	proto.setActivedcId(cluster.getActivedcId());
    	proto.setClusterDescription(cluster.getClusterDescription());
    	proto.setStatus(ClusterStatus.Normal.toString());
			proto.setIsXpipeInterested(true);
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		if(!checkEmails(cluster.getClusterAdminEmails())) {
			throw new IllegalArgumentException("Emails should be ctrip emails and separated by comma or semicolon");
		}
    	proto.setClusterAdminEmails(cluster.getClusterAdminEmails());
		proto.setClusterOrgId(getOrgIdFromClusterOrgName(cluster));

    	final ClusterTbl queryProto = proto;
    	ClusterTbl result =  queryHandler.handleQuery(new DalQuery<ClusterTbl>(){
			@Override
			public ClusterTbl doQuery() throws DalException {
				return clusterDao.createCluster(queryProto);
			}
    	});

    	if(slaveDcs != null){
			for(DcTbl dc : slaveDcs) {
				bindDc(cluster.getClusterName(), dc.getDcName());
			}
		}

    	if(shards != null){
			for (ShardModel shard : shards) {
				shardService.createShard(cluster.getClusterName(), shard.getShardTbl(), shard.getSentinels());
			}
		}

    	return result;
	}

	public long getOrgIdFromClusterOrgName(ClusterTbl cluster) {
		String orgName = cluster.getClusterOrgName();
		OrganizationTbl organizationTbl = organizationService.getOrgByName(orgName);
		if(organizationTbl == null)
			return 0L;
		Long id = organizationTbl.getId();
		return id == null ? 0L : id;
	}

	@Override
	public ClusterTbl findClusterAndOrg(String clusterName) {
		ClusterTbl clusterTbl = clusterDao.findClusterAndOrgByName(clusterName);
		OrganizationTbl organizationTbl = clusterTbl.getOrganizationInfo();
		if(organizationTbl != null) {
			clusterTbl.setClusterOrgName(organizationTbl.getOrgName());
		}
		// Set null if no organization bind with cluster
		if(organizationTbl == null || organizationTbl.getId() == null) {
			clusterTbl.setOrganizationInfo(null);
		}
		return clusterTbl;
	}

	@Override
	public List<ClusterTbl> findClustersWithOrgInfoByActiveDcId(long activeDc) {
		List<ClusterTbl> result = clusterDao.findClustersWithOrgInfoByActiveDcId(activeDc);
		result = fillClusterOrgName(result);
		return setOrgNullIfNoOrgIdExsits(result);
	}

	@Override public List<ClusterTbl> findAllClustersWithOrgInfo() {
		List<ClusterTbl> result = clusterDao.findAllClusterWithOrgInfo();
		result = fillClusterOrgName(result);
		return setOrgNullIfNoOrgIdExsits(result);
	}

	private List<ClusterTbl> fillClusterOrgName(List<ClusterTbl> clusterTblList) {
		for(ClusterTbl cluster : clusterTblList) {
			cluster.setClusterOrgName(cluster.getOrganizationInfo().getOrgName());
		}
		return clusterTblList;
	}

	private List<ClusterTbl> setOrgNullIfNoOrgIdExsits(List<ClusterTbl> clusterTblList) {
		for(ClusterTbl cluster : clusterTblList) {
			OrganizationTbl organizationTbl = cluster.getOrganizationInfo();
			if(organizationTbl.getId() == null) {
				cluster.setOrganizationInfo(null);
			}
		}
		return clusterTblList;
	}

	@Override
	public void updateCluster(String clusterName, ClusterTbl cluster) {
		ClusterTbl proto = find(clusterName);
    	if(null == proto) throw new BadRequestException("Cannot find cluster");

		if(proto.getId() != cluster.getId()) {
			throw new BadRequestException("Cluster not match.");
		}
		proto.setClusterDescription(cluster.getClusterDescription());
		proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		if(!checkEmails(cluster.getClusterAdminEmails())) {
			throw new IllegalArgumentException("Emails should be ctrip emails and separated by comma or semicolon");
		}
		proto.setClusterAdminEmails(cluster.getClusterAdminEmails());
		proto.setClusterOrgId(getOrgIdFromClusterOrgName(cluster));
		// organization info should not be updated by cluster,
		// it's automatically updated by scheduled task
		proto.setOrganizationInfo(null);
		
		final ClusterTbl queryProto = proto;
    	clusterDao.updateCluster(queryProto);
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
	public void updateStatusById(long id, ClusterStatus clusterStatus) {

		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(id);
		clusterTbl.setStatus(clusterStatus.toString());

		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateStatusById(clusterTbl, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void deleteCluster(String clusterName) {
		ClusterTbl proto = find(clusterName);
    	if(null == proto) throw new BadRequestException("Cannot find cluster");
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	List<DcTbl> relatedDcs = dcService.findClusterRelatedDc(clusterName);
    	
    	final ClusterTbl queryProto = proto;

    	// Call cluster delete event
		ClusterEvent clusterEvent = clusterDeleteEventFactory.createClusterEvent(clusterName);

		try {
			clusterDao.deleteCluster(queryProto);
		} catch (Exception e) {
			throw new ServerException(e.getMessage());
		}

    	clusterEvent.onEvent();

    	/** Notify meta server **/
    	notifier.notifyClusterDelete(clusterName, relatedDcs);
	}

	@Override
	public void bindDc(String clusterName, String dcName) {
		final ClusterTbl cluster = find(clusterName);
    	final DcTbl dc = dcService.find(dcName);
    	if(null == dc || null == cluster) throw new BadRequestException("Cannot bind dc due to unknown dc or cluster");
    	
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.bindDc(cluster, dc);
			}
    	});
	}

	@Override
	public void unbindDc(String clusterName, String dcName) {
		final ClusterTbl cluster = find(clusterName);
    	final DcTbl dc = dcService.find(dcName);
    	if(null == dc || null == cluster) throw new BadRequestException("Cannot unbind dc due to unknown dc or cluster");
    	
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.unbindDc(cluster, dc);
			}
    	});
    	
    	/** Notify meta server **/
    	notifier.notifyClusterDelete(clusterName, Arrays.asList(new DcTbl[]{dc}));
    	
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

	public boolean checkEmails(String emails) {
		if(emails == null || emails.trim().isEmpty()) {
			return false;
		}
		String splitter = "\\s*(,|;)\\s*";
		String[] emailStrs = StringUtil.splitRemoveEmpty(splitter, emails);
		for(String email : emailStrs) {
			EmailService.CheckEmailResponse response = EmailService.DEFAULT.checkEmailAddress(email);
			if(!response.isOk())
				return false;
		}
		return true;
	}



	/**
	 * Randomly re-balance sentinel assignment for clusters among dcs
     * */
	@Override
	public List<String> reBalanceSentinels(final int numOfClusters) {
		List<String> clusters = randomlyChosenClusters(findAllClusterNames(), numOfClusters);
		logger.info("[reBalanceSentinels] pick up clusters: {}", clusters);

		reBalanceClusterSentinels(clusters);
		return clusters;
	}

    // randomly findRedisHealthCheckInstance 'numOfClusters' cluster names
	private List<String> randomlyChosenClusters(final List<String> clusters, final int num) {
		if(clusters == null || clusters.isEmpty() || num >= clusters.size()) return clusters;
		if(random == null) {
			random = new Random();
		}
		int bound = clusters.size(), index = random.nextInt(bound);
		Set<String> result = new HashSet<>();
		for(int count = 0; count < num; count++) {
			while (index < 0 || !result.add(clusters.get(index))) {
				index = random.nextInt(bound);
			}
		}
		return new LinkedList<>(result);
	}

	public void reBalanceClusterSentinels(final List<String> clusters) {
        List<String> dcNames = dcService.findAllDcNames().stream()
                .map(dcTbl -> dcTbl.getDcName()).collect(Collectors.toList());
        Map<String, List<SetinelTbl>> dcToSentinels = getDcNameMappedSentinels(dcNames);

		// maxChangeOnce must be smaller than DefaultDcMetaCache.META_MODIFY_PROTECT_COUNT
		int maxChangeOnce = consoleConfig.getRebalanceSentinelMaxNumOnce(),
				changingPeriod = consoleConfig.getRebalanceSentinelInterval();
		if(clusters.size() < maxChangeOnce) {
			for (String cluster : clusters) {
				balanceCluster(dcToSentinels, cluster);
			}
		} else {
			List<String> nextExecutionClusters = clusters.subList(maxChangeOnce, clusters.size());
			for(int i = 0; i < maxChangeOnce; i++) {
				balanceCluster(dcToSentinels, clusters.get(i));
			}
			scheduled.schedule(() -> reBalanceClusterSentinels(nextExecutionClusters), changingPeriod, TimeUnit.SECONDS);
		}
	}

	// Cache {dc name} -> List {SentinelTbl}
	private Map<String, List<SetinelTbl>> getDcNameMappedSentinels(final List<String> dcNames) {
	    Map<String, List<SetinelTbl>> map = Maps.newHashMap();
	    for(String dc : dcNames) {
            List<SetinelTbl> sentinels = sentinelService.findAllByDcName(dc);
            map.put(dc, sentinels);
        }
        return map;
    }

    // Add transaction for one cluster update, rollback if one 'DcClusterShard' update fails
	@VisibleForTesting
	@DalTransaction
	protected void balanceCluster(Map<String, List<SetinelTbl>> dcToSentinels, final String cluster) {

		for(String dcName : dcToSentinels.keySet()) {
			List<DcClusterShardTbl> dcClusterShards = dcClusterShardService.findAllByDcCluster(dcName, cluster);
			List<SetinelTbl> sentinels = dcToSentinels.get(dcName);
			if(dcClusterShards == null || sentinels == null || sentinels.isEmpty()) {
				throw new XpipeRuntimeException("DcClusterShard | Sentinels should not be null");
			}
            long randomlySelectedSentinelId = randomlyChoseSentinels(sentinels);
			dcClusterShards.forEach(dcClusterShard -> {
				dcClusterShard.setSetinelId(randomlySelectedSentinelId);
				try {
					dcClusterShardService.updateDcClusterShard(dcClusterShard);
				} catch (DalException e) {
					throw new XpipeRuntimeException(e.getMessage());
				}
			});
		}
	}

	@VisibleForTesting
	protected long randomlyChoseSentinels(List<SetinelTbl> sentinels) {
		int randomNum = Math.abs(random.nextInt());
		int randomIndex = randomNum % sentinels.size();
		return sentinels.get(randomIndex).getSetinelId();
	}

	@Override
	public List<ClusterListClusterModel> findUnhealthyClusters() {
		try {
			XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
			if(xpipeMeta == null || xpipeMeta.getDcs() == null) {
				return Collections.emptyList();
			}

			String prefix = "健康监测有问题的shard及redis:\n";

			Map<String, ClusterListClusterModel> unhealthyClusters = Maps.newHashMap();
			for(DcMeta dcMeta: xpipeMeta.getDcs().values()) {
				for(ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
					StringBuffer sb = new StringBuffer();
					for(ShardMeta shardMeta : clusterMeta.getShards().values()) {
						boolean shardLogged = false;
						for(RedisMeta redisMeta : shardMeta.getRedises()) {
							HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
							long delay = delayService.getDelay(hostPort);
							if(delay == TimeUnit.NANOSECONDS.toMillis(DelayAction.SAMPLE_LOST_AND_NO_PONG)
									|| delay == TimeUnit.NANOSECONDS.toMillis(DelayAction.SAMPLE_LOST_BUT_PONG)
									) {

								String clusterName = clusterMeta.getId();
								unhealthyClusters.putIfAbsent(clusterName,
										new ClusterListClusterModel(clusterMeta.getId()));
								if(!shardLogged) {
									shardLogged = true;
									sb.append(shardMeta.getId()).append(": ");
								}
								sb.append(hostPort.toString()).append(", ");
							}
						}
						if(shardLogged) {
							sb.append(";\n");
						}
					}
					ClusterListClusterModel cluster = unhealthyClusters.get(clusterMeta.getId());
					if(cluster != null) {
						String message = cluster.getMessage() == null ? "" : cluster.getMessage();
						message += sb.toString();
						if(!message.startsWith(prefix)) {
							message = prefix + message;
						}
						cluster.setMessage(message);
					}
				}
			}
			return richClusterInfo(unhealthyClusters);
		} catch (Exception e) {
			logger.error("[findUnhealthyClusters]{}", e);
			return Collections.emptyList();
		}
	}

	@VisibleForTesting
	protected List<ClusterListClusterModel> richClusterInfo(Map<String, ClusterListClusterModel> clusters) {

		if(clusters.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> clusterNames = Lists.newArrayListWithExpectedSize(clusters.size());
		clusterNames.addAll(clusters.keySet());
		List<ClusterTbl> clusterTbls = clusterDao.findClustersWithName(clusterNames);

		List<ClusterListClusterModel> result = Lists.newArrayListWithExpectedSize(clusterTbls.size());

		for(ClusterTbl clusterTbl : clusterTbls) {
			ClusterListClusterModel cluster = clusters.get(clusterTbl.getClusterName());
			cluster.setActivedcId(clusterTbl.getActivedcId())
					.setClusterAdminEmails(clusterTbl.getClusterAdminEmails())
					.setClusterDescription(clusterTbl.getClusterDescription());
			result.add(cluster);
		}
		return result;
	}

	@VisibleForTesting
	protected void setClusterDao(ClusterDao clusterDao) {
		this.clusterDao = clusterDao;
	}

	@Override
	public List<ClusterTbl> findAllClusterByDcNameBind(String dcName){
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
	public List<ClusterTbl> findAllClustersByDcName(String dcName){
		if (StringUtil.isEmpty(dcName))
			return Collections.emptyList();

		return findClustersWithOrgInfoByActiveDcId(dcService.find(dcName).getId());
	}
}
