package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.MigrationResources;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Lists;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

@Repository
public class MigrationEventDao extends AbstractXpipeConsoleDAO {

	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	@Autowired
	private RedisService redisService;
	@Autowired
	private MigrationService migrationService;

	@Resource( name = MigrationResources.MIGRATION_EXECUTOR )
	private Executor executors;

	@Resource( name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR )
	private ScheduledExecutorService scheduled;


	private MigrationEventTblDao migrationEventTblDao;
	private MigrationClusterTblDao migrationClusterTblDao;
	private MigrationShardTblDao migrationShardTblDao;
	private ClusterTblDao clusterTblDao;
	private ShardTblDao shardTblDao;

	@PostConstruct
	private void postConstruct() {
		try {
			migrationEventTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationEventTblDao.class);
			migrationClusterTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationClusterTblDao.class);
			migrationShardTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationShardTblDao.class);
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	public List<MigrationClusterModel> getMigrationCluster(final long eventId) {

		List<MigrationClusterModel> res = new LinkedList<>();

		Map<Long, String> dcMap = dcService.dcNameMap();

		List<MigrationClusterTbl> migrationClusterTbls = queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
			@Override
			public List<MigrationClusterTbl> doQuery() throws DalException {
				return migrationClusterTblDao.findByEventId(eventId, MigrationClusterTblEntity.READSET_FULL_ALL);
			}
		});

		for(MigrationClusterTbl migrationClusterTbl : migrationClusterTbls) {

			MigrationClusterModel model = new MigrationClusterModel();
			ClusterTbl cluster = migrationClusterTbl.getCluster();

			model.setMigrationCluster(new MigrationClusterInfo(cluster.getClusterName(), dcMap, migrationClusterTbl));
			
			List<MigrationShardTbl> migrationShardTbls = queryHandler.handleQuery(new DalQuery<List<MigrationShardTbl>>() {
				@Override
				public List<MigrationShardTbl> doQuery() throws DalException {
					return migrationShardTblDao.findByMigrationClusterId(migrationClusterTbl.getId(), MigrationShardTblEntity.READSET_FULL_ALL);
				}
			});
			for(MigrationShardTbl migrationShardTbl : migrationShardTbls) {
				MigrationShardModel shardModel = new MigrationShardModel();
				shardModel.setMigrationShard(migrationShardTbl);
				
				model.addMigrationShard(shardModel);
			}
			
			res.add(model);
		}
		
		return res;
	}
	
	public MigrationEvent buildMigrationEvent(final long eventId) {
		List<MigrationEventTbl> eventDetails = queryHandler.handleQuery(new DalQuery<List<MigrationEventTbl>>() {
			@Override
			public List<MigrationEventTbl> doQuery() throws DalException {
				return migrationEventTblDao.findWithAllDetails(eventId, MigrationEventTblEntity.READSET_FULL_ALL);
			}
		});
		return loadMigrationEvent(eventDetails);
	}

	@DalTransaction
	public MigrationEvent createMigrationEvent(MigrationRequest migrationRequest) {

		if (null != migrationRequest) {
			/** Create event **/
			MigrationEventTbl migrationEvent = migrationEventTblDao.createLocal();
			migrationEvent.setOperator(migrationRequest.getUser()).setEventTag(migrationRequest.getTag());

			queryHandler.handleQuery(new DalQuery<MigrationEventTbl>() {
				@Override
				public MigrationEventTbl doQuery() throws DalException {
					migrationEventTblDao.insert(migrationEvent);
					return migrationEvent;
				}
			});

			/** Create migration clusters task **/
			final List<MigrationClusterTbl> migrationClusters = createMigrationClusters(migrationEvent.getId(),
					migrationRequest.getRequestClusters());

			/** Create migration shards task **/
			createMigrationShards(migrationClusters);

			/** Notify event manager **/
			return buildMigrationEvent(migrationEvent.getId());
		} else {
			throw new BadRequestException("Cannot create migration event from nothing!");
		}
	}
	
	public List<Long> findAllUnfinished() {

		List<MigrationEventTbl> migrationEventTbls = queryHandler.handleQuery(new DalQuery<List<MigrationEventTbl>>() {
			@Override
			public List<MigrationEventTbl> doQuery() throws DalException {
				return migrationEventTblDao.findUnfinishedEvents(MigrationEventTblEntity.READSET_FULL);
			}
		});

		List<Long> result = new LinkedList<>();
		Set<Long> distinct = new HashSet<>();

		for(MigrationEventTbl migrationEventTbl : migrationEventTbls){

			Long id = migrationEventTbl.getId();
			if(distinct.add(id)){
				result.add(id);
			}else{
				logger.info("[findAllUnfinished][already exist]{}", id);
			}
		}
		return result;
	}
	
	private MigrationEvent loadMigrationEvent(List<MigrationEventTbl> details) {

		if(!CollectionUtils.isEmpty(details)) {

			MigrationEvent event = new DefaultMigrationEvent(details.get(0));
			for(MigrationEventTbl detail : details) {
				MigrationClusterTbl cluster = detail.getRedundantClusters();
				MigrationShardTbl shard = detail.getRedundantShards();
				
				if(null == event.getMigrationCluster(cluster.getClusterId())) {
					event.addMigrationCluster(new DefaultMigrationCluster(executors, scheduled, event, detail.getRedundantClusters(),
							dcService, clusterService, shardService, redisService, migrationService));
				}
				MigrationCluster migrationCluster = event.getMigrationCluster(cluster.getClusterId()); 
				migrationCluster.addNewMigrationShard(new DefaultMigrationShard(migrationCluster, shard,
						migrationCluster.getClusterShards().get(shard.getShardId()),
						migrationCluster.getClusterDcs(),
						migrationService));
			}
			
			return event;
		}
		throw new BadRequestException("Cannot load migration event from null.");
	}

	private List<MigrationClusterTbl> createMigrationClusters(final long eventId, List<MigrationRequest.ClusterInfo> migrationClusters) {
		final List<MigrationClusterTbl> toCreateMigrationCluster = new LinkedList<>();

		if (null != migrationClusters) {
			for (MigrationRequest.ClusterInfo migrationCluster : migrationClusters) {

				lockCluster(migrationCluster.getClusterId());
				MigrationClusterTbl proto = migrationClusterTblDao.createLocal();
				proto.setMigrationEventId(eventId).
						setClusterId(migrationCluster.getClusterId()).
						setSourceDcId(migrationCluster.getFromDcId())
						.setDestinationDcId(migrationCluster.getToDcId())
						.setStatus(MigrationStatus.Initiated.toString()).setPublishInfo("");
				toCreateMigrationCluster.add(proto);
			}
		}

		return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
			@Override
			public List<MigrationClusterTbl> doQuery() throws DalException {
				migrationClusterTblDao.insertBatch(Lists.toArray(MigrationClusterTbl.class, toCreateMigrationCluster));
				return migrationClusterTblDao.findByEventId(eventId, MigrationClusterTblEntity.READSET_FULL);
			}
		});
	}
	
	private void lockCluster(final long clusterId) {

		ClusterTbl cluster = queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return clusterTblDao.findByPK(clusterId, ClusterTblEntity.READSET_FULL);
			}
		});
		if(null == cluster) throw new BadRequestException(String.format("Cluster:%s do not exist!", clusterId));
		
		if(!cluster.getStatus().toLowerCase().equals(ClusterStatus.Normal.toString().toLowerCase())) {
			throw new BadRequestException(String.format("Cluster:%s already under migrating tasks!Please verify it first!", cluster.getClusterName()));
		} else {
			cluster.setStatus(ClusterStatus.Lock.toString());
		}
		
		final ClusterTbl proto = cluster;
		queryHandler.handleQuery(new DalQuery<Void>() {
			@Override
			public Void doQuery() throws DalException {
				clusterTblDao.updateByPK(proto, ClusterTblEntity.UPDATESET_FULL);
				return null;
			}
			
		});
	}

	private void createMigrationShards(List<MigrationClusterTbl> migrationClusters) {
		final List<MigrationShardTbl> toCreateMigrationShards = new LinkedList<>();

		if (null != migrationClusters) {
			for (final MigrationClusterTbl migrationCluster : migrationClusters) {
				List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
					@Override
					public List<ShardTbl> doQuery() throws DalException {
						return shardTblDao.findAllByClusterId(migrationCluster.getClusterId(),
								ShardTblEntity.READSET_FULL);
					}
				});

				if (null != shards) {
					for (ShardTbl shard : shards) {
						MigrationShardTbl migrationShardProto = migrationShardTblDao.createLocal();
						migrationShardProto.setMigrationClusterId(migrationCluster.getId()).setShardId(shard.getId())
						.setLog("");
						toCreateMigrationShards.add(migrationShardProto);
					}
				}
			}
		}

		queryHandler.handleQuery(new DalQuery<Void>() {
			@Override
			public Void doQuery() throws DalException {
				migrationShardTblDao.insertBatch(Lists.toArray(MigrationShardTbl.class, toCreateMigrationShards));
				return null;
			}
		});
	}
}
