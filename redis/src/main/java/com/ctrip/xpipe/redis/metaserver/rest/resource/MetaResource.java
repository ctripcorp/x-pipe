package com.ctrip.xpipe.redis.metaserver.rest.resource;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.DcMeta;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;
import com.ctrip.xpipe.redis.keeper.entity.XpipeMeta;
import com.ctrip.xpipe.redis.keeper.meta.ShardStatus;
import com.ctrip.xpipe.redis.metaserver.MetaHolder;
import com.ctrip.xpipe.redis.metaserver.MetaServer;
import com.ctrip.xpipe.utils.ServicesUtil;

@Path("/api/v1")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource extends BaseRecource {

	private Logger log = LoggerFactory.getLogger(MetaResource.class);

	private static final FoundationService foundationService = ServicesUtil.getFoundationService();

	@Path("/{clusterId}/{shardId}")
	@GET
	public Response getClusterStatus(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
			@QueryParam("version") @DefaultValue("0") long version) {
		// TODO merge and update cluster status on demand

		KeeperMeta activeKeeper = doGetActiveKeeper(clusterId, shardId);
		RedisMeta redisMaster = doGetRedisMaster(clusterId, shardId);
		KeeperMeta upstreamKeeper = doGetUpstreamKeeper(clusterId, shardId);

		return Response.status(Status.OK).entity(new ShardStatus(activeKeeper, upstreamKeeper, redisMaster)).build();
	}

	@Path("/{clusterId}/{shardId}/keeper/master")
	@GET
	public Response getActiveKeeper(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
			@QueryParam("version") @DefaultValue("0") long version, @QueryParam("format") @DefaultValue("json") String format) {
		KeeperMeta keeper = doGetActiveKeeper(clusterId, shardId);

		if (keeper == null) {
			return Response.status(Status.OK).entity("").build();
		} else {
			if ("plain".equals(format)) {
				String plainRes = String.format("%s:%s", keeper.getIp(), keeper.getPort());
				return Response.status(Status.OK).entity(plainRes).build();
			} else {
				return Response.status(Status.OK).entity(keeper).build();
			}
		}
	}

	private KeeperMeta doGetActiveKeeper(String clusterId, String shardId) {
		// TODO call getBean only once
		MetaServer metaServer = getSpringContext().getBean(MetaServer.class);
		KeeperMeta keeper = metaServer.getActiveKeeper(clusterId, shardId);
		return keeper;
	}

	@Path("/{clusterId}/{shardId}/keeper/upstream")
	@GET
	public Response getUpstreamKeeper(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId) {
		KeeperMeta upstreamKeeper = doGetUpstreamKeeper(clusterId, shardId);

		return Response.status(Status.OK).entity(upstreamKeeper).build();
	}

	private KeeperMeta doGetUpstreamKeeper(String clusterId, String shardId) {
		MetaHolder metaHolder = getSpringContext().getBean(MetaHolder.class);
		XpipeMeta meta = metaHolder.getMeta();

		String thisDc = foundationService.getDataCenter();
		ShardMeta activeShard = null;
		for (DcMeta dc : meta.getDcs().values()) {
			if (thisDc.equals(dc.getId())) {
				// upstream keeper should locate in another dc
				continue;
			}

			ClusterMeta cluster = dc.findCluster(clusterId);
			if (cluster != null) {
				ShardMeta shard = cluster.findShard(shardId);
				if (shard != null && dc.getId().equals(shard.getActiveDc())) {
					activeShard = shard;
					break;
				}
			}
		}

		KeeperMeta upstreamKeeper = null;
		if (activeShard != null) {
			for (KeeperMeta keeper : activeShard.getKeepers()) {
				if (keeper.isActive()) {
					upstreamKeeper = keeper;
					break;
				}
			}
		}
		return upstreamKeeper;
	}

	@Path("/{clusterId}/{shardId}/redis/master")
	@GET
	public Response getRedisMaster(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
			@QueryParam("version") @DefaultValue("0") long version, @QueryParam("format") @DefaultValue("json") String format) {

		RedisMeta master = doGetRedisMaster(clusterId, shardId);

		if (master == null) {
			return Response.status(Status.OK).entity("").build();
		} else {
			if ("plain".equals(format)) {
				String plainRes = String.format("%s:%s", master.getIp(), master.getPort());
				return Response.status(Status.OK).entity(plainRes).build();
			} else {
				return Response.status(Status.OK).entity(master).build();
			}
		}
	}

	private RedisMeta doGetRedisMaster(String clusterId, String shardId) {
		MetaHolder metaHolder = getSpringContext().getBean(MetaHolder.class);
		RedisMeta master = findRedisMaster(metaHolder.getMeta(), clusterId, shardId);
		return master;
	}

	private RedisMeta findRedisMaster(XpipeMeta meta, String clusterId, String shardId) {
		ShardMeta shard = findShard(meta, clusterId, shardId);

		for (RedisMeta redis : shard.getRedises()) {
			if (redis.isMaster()) {
				// TODO
				redis.setShardActive(foundationService.getDataCenter().equals(shard.getActiveDc()));
				return redis;
			}
		}

		return null;
	}

	private ShardMeta findShard(XpipeMeta meta, String clusterId, String shardId) {
		DcMeta dc = meta.findDc(foundationService.getDataCenter());
		if (dc == null) {
			log.warn("Meta for DC {} not found", foundationService.getDataCenter());
			return null;
		}

		ClusterMeta cluster = dc.findCluster(clusterId);
		if (cluster == null) {
			log.warn("Meta for cluster {}, DC {} not found", clusterId, foundationService.getDataCenter());
			return null;
		}

		ShardMeta shard = cluster.findShard(shardId);
		if (shard == null) {
			log.warn("Meta for shard {}, cluster {}, DC {} not found", shardId, clusterId, foundationService.getDataCenter());
			return null;
		}
		return shard;
	}

}
