package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.MigrationResources;
import com.ctrip.xpipe.redis.console.migration.command.ReactorMigrationCommandBuilderImpl;
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
import com.ctrip.xpipe.utils.VisibleForTesting;
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

	@Resource( name = MigrationResources.MIGRATION_IO_CALLBACK_EXECUTOR )
	private Executor ioCallbackExecutors;

	@Resource( name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR )
	private ScheduledExecutorService scheduled;

	@Autowired
	private ReactorMigrationCommandBuilderImpl reactorMigrationCommandBuilder;


	private MigrationEventTblDao migrationEventTblDao;
	private MigrationClusterTblDao migrationClusterTblDao;
	private MigrationShardTblDao migrationShardTblDao;
	private ClusterTblDao clusterTblDao;
	private ShardTblDao shardTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;

	@PostConstruct
	private void postConstruct() {
		try {
			migrationEventTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationEventTblDao.class);
			migrationClusterTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationClusterTblDao.class);
			migrationShardTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationShardTblDao.class);
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
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
				DefaultMigrationShard migrationShard = new DefaultMigrationShard(migrationCluster, shard,
						migrationCluster.getClusterShards().get(shard.getShardId()),
						migrationCluster.getClusterDcs(),
						migrationService, reactorMigrationCommandBuilder);
				migrationShard.setExecutors(ioCallbackExecutors);
				migrationCluster.addNewMigrationShard(migrationShard);
			}
			
			return event;
		}
		throw new BadRequestException("Cannot load migration event from null.");
	}

	private List<MigrationClusterTbl> createMigrationClusters(final long eventId, List<MigrationRequest.ClusterInfo> migrationClusters) {
		final List<MigrationClusterTbl> toCreateMigrationCluster = new LinkedList<>();

		if (null != migrationClusters) {
			for (MigrationRequest.ClusterInfo migrationCluster : migrationClusters) {

				lockCluster(migrationCluster.getClusterId(), eventId);
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
	
	private void lockCluster(final long clusterId, final long eventId) {
		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(clusterId);
		clusterTbl.setOriginStatus(ClusterStatus.Normal.toString());
		clusterTbl.setStatus(ClusterStatus.Lock.toString());
		clusterTbl.setMigrationEventId(eventId);

		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterTblDao.atomicSetStatus(clusterTbl, ClusterTblEntity.UPDATESET_MIGRATION_STATUS);
			}
		});

	}

	private void createMigrationShards(List<MigrationClusterTbl> migrationClusters) {
		final List<MigrationShardTbl> toCreateMigrationShards = new LinkedList<>();

		if (null != migrationClusters) {
			for (final MigrationClusterTbl migrationCluster : migrationClusters) {
				List<DcClusterShardTbl> /* distinct */ withShardIds = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
					@Override
					public List<DcClusterShardTbl> doQuery() throws DalException {
						return dcClusterShardTblDao.findByClusterIdForDrSwitch(migrationCluster.getClusterId(),
								DcClusterShardTblEntity.READSET_SHARD_ID);
					}
				});

				if (null != withShardIds) {
					for (DcClusterShardTbl withShardId : withShardIds) {
						MigrationShardTbl migrationShardProto = migrationShardTblDao.createLocal();
						migrationShardProto.setMigrationClusterId(migrationCluster.getId()).setShardId(withShardId.getShardId())
						.setLog("");
						toCreateMigrationShards.add(migrationShardProto);
					}
				}
			}
		}

		queryHandler.handleBatchInsert(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return migrationShardTblDao.insertBatch(Lists.toArray(MigrationShardTbl.class, toCreateMigrationShards));
			}
		});
	}

	@VisibleForTesting
	protected void setClusterTblDao(ClusterTblDao clusterTblDao) {
		this.clusterTblDao = clusterTblDao;
	}

}
