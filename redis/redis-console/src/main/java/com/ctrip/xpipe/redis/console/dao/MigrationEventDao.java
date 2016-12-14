package com.ctrip.xpipe.redis.console.dao;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Lists;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblEntity;
import com.ctrip.xpipe.redis.console.model.MigrationShardModel;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationShardTblEntity;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

@Repository
public class MigrationEventDao extends AbstractXpipeConsoleDAO {
	@Autowired
	private UserInfoHolder userInfo;
	@Autowired
	private MigrationEventManager eventManager;
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	@Autowired
	private MigrationService migrationService;

	private MigrationEventTblDao migrationEventTblDao;
	private MigrationClusterTblDao migrationClusterTblDao;
	private MigrationShardTblDao migrationShardTblDao;
	private ClusterTblDao clusterTblDao;
	private ShardTblDao shardTblDao;
	
	private ExecutorService cachedThreadPool = Executors.newCachedThreadPool(XpipeThreadFactory.create("MigrationEventDao"));

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
		
		List<MigrationClusterTbl> migrationClusterTbls = queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
			@Override
			public List<MigrationClusterTbl> doQuery() throws DalException {
				return migrationClusterTblDao.findByEventId(eventId, MigrationClusterTblEntity.READSET_FULL_ALL);
			}
		});
		for(MigrationClusterTbl migrationClusterTbl : migrationClusterTbls) {
			MigrationClusterModel model = new MigrationClusterModel();
			model.setMigrationCluster(migrationClusterTbl);
			
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
	public long createMigrationEvnet(MigrationEventModel event) {
		if (null != event) {
			/** Create event **/
			MigrationEventTbl proto = migrationEventTblDao.createLocal();
			final String eventTag = generateUniqueEventTag(userInfo.getUser().getUserId());
			proto.setOperator(userInfo.getUser().getUserId()).setEventTag(eventTag);
			final MigrationEventTbl forCreate = proto;
			final MigrationEventTbl result = queryHandler.handleQuery(new DalQuery<MigrationEventTbl>() {
				@Override
				public MigrationEventTbl doQuery() throws DalException {
					migrationEventTblDao.insert(forCreate);
					return migrationEventTblDao.findByTag(eventTag, MigrationEventTblEntity.READSET_FULL);
				}
			});

			/** Create migration clusters task **/
			final List<MigrationClusterTbl> migrationClusters = createMigrationClusters(result.getId(),
					event.getEvent().getMigrationClusters());

			/** Create migration shards task **/
			createMigrationShards(migrationClusters);

			/** Notify event manager **/
			cachedThreadPool.submit(new Runnable() {
				@Override
				public void run() {
					eventManager.addEvent(buildMigrationEvent(result.getId()));
				}
			});
			
			return result.getId();
		} else {
			throw new BadRequestException("Cannot create migration event from nothing!");
		}
	}
	
	public List<MigrationEventTbl> findAllUnfinished() {
		return queryHandler.handleQuery(new DalQuery<List<MigrationEventTbl>>() {
			@Override
			public List<MigrationEventTbl> doQuery() throws DalException {
				return migrationEventTblDao.findUnfinishedEvents(MigrationEventTblEntity.READSET_FULL);
			}
		});
	}
	
	private MigrationEvent loadMigrationEvent(List<MigrationEventTbl> details) {
		if(! CollectionUtils.isEmpty(details)) {
			MigrationEvent event = new DefaultMigrationEvent(details.get(0));
			for(MigrationEventTbl detail : details) {
				MigrationClusterTbl cluster = detail.getRedundantClusters();
				MigrationShardTbl shard = detail.getRedundantShards();
				
				if(MigrationStatus.isTerminated(cluster.getStatus())) {
					continue;
				}
				if(null == event.getMigrationCluster(cluster.getId())) {
					event.addMigrationCluster(new DefaultMigrationCluster(detail.getRedundantClusters(),
							dcService, clusterService, shardService, migrationService));
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

	private List<MigrationClusterTbl> createMigrationClusters(final long eventId, List<MigrationClusterTbl> migrationClusters) {
		final List<MigrationClusterTbl> toCreateMigrationCluster = new LinkedList<>();

		if (null != migrationClusters) {
			for (MigrationClusterTbl migrationCluster : migrationClusters) {
				updateClusterStatus(migrationCluster.getClusterId(), ClusterStatus.Lock);
				
				MigrationClusterTbl proto = migrationClusterTblDao.createLocal();
				proto.setMigrationEventId(eventId).setClusterId(migrationCluster.getClusterId())
						.setDestinationDcId(migrationCluster.getDestinationDcId()).setStatus(MigrationStatus.Initiated.toString());
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
	
	private void updateClusterStatus(final long clusterId, ClusterStatus status) {
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

	private String generateUniqueEventTag(String user) {
		StringBuilder sb = new StringBuilder();
		sb.append(DataModifiedTimeGenerator.generateModifiedTime());
		sb.append("-");
		sb.append(user);
		return sb.toString();
	}
}
