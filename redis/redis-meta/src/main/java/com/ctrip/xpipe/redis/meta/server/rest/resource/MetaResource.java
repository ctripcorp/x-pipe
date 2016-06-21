package com.ctrip.xpipe.redis.meta.server.rest.resource;


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

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.meta.server.MetaServer;




@Path("/api/v1")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource extends BaseRecource {
	
	public static String FAKE_ADDRESS = "0.0.0.0:0";

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

	@Path("/{clusterId}/{shardId}/keeper/upstream")
	@GET
	public Response getUpstreamKeeper(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId) {
		KeeperMeta upstreamKeeper = doGetUpstreamKeeper(clusterId, shardId);

		return Response.status(Status.OK).entity(upstreamKeeper).build();
	}

	@Path("/{clusterId}/{shardId}/redis/master")
	@GET
	public Response getRedisMaster(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
			@QueryParam("version") @DefaultValue("0") long version, @QueryParam("format") @DefaultValue("json") String format) {

		RedisMeta master = doGetRedisMaster(clusterId, shardId);

		if ("plain".equals(format)) {
			if (master == null) {
				return Response.status(Status.OK).entity(FAKE_ADDRESS).build();
			}
			String plainRes = String.format("%s:%s", master.getIp(), master.getPort());
			return Response.status(Status.OK).entity(plainRes).build();
		} else {
			return Response.status(Status.OK).entity(master == null ? "": master).build();
		}
	}

	private KeeperMeta doGetActiveKeeper(String clusterId, String shardId) {
		KeeperMeta keeper = getMetaServer().getActiveKeeper(clusterId, shardId);
		return keeper;
	}

	private KeeperMeta doGetUpstreamKeeper(String clusterId, String shardId) {
		return getMetaServer().getUpstreamKeeper(clusterId, shardId);
	}

	private RedisMeta doGetRedisMaster(String clusterId, String shardId) {
		return getMetaServer().getRedisMaster(clusterId, shardId);
	}

	private MetaServer getMetaServer() {
		// TODO call getBean only once
		return getSpringContext().getBean(MetaServer.class);
	}

}
