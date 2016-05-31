package com.ctrip.xpipe.redis.metaserver.rest.resource;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.keeper.entity.Keeper;
import com.ctrip.xpipe.redis.keeper.entity.Redis;
import com.ctrip.xpipe.redis.metaserver.MetaServer;

@Path("/api/v1")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource extends BaseRecource {

	private Logger log = LoggerFactory.getLogger(MetaResource.class);

	private Redis master = new Redis();

	public MetaResource() {
		master.setMaster(true);
		master.setIp("127.0.0.1");
		master.setPort(6379);
	}

	@Path("/{clusterId}/{shardId}/keeper/master")
	@GET
	public Response getActiveKeeper(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
	      @QueryParam("version") @DefaultValue("0") long version,
	      @QueryParam("format") @DefaultValue("json") String format) {
		MetaServer metaServer = getSpringContext().getBean(MetaServer.class);
		Keeper keeper = metaServer.getActiveKeeper(clusterId, shardId);

		if ("plain".equals(format)) {
			String plainRes = String.format("%s:%s", keeper.getIp(), keeper.getPort());
			return Response.status(Status.OK).entity(plainRes).build();
		} else {
			return Response.status(Status.OK).entity(keeper).build();
		}
	}

	@Path("/{clusterId}/{shardId}/redis/master")
	@GET
	public Response getRedisMaster(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
	      @QueryParam("version") @DefaultValue("0") long version,
	      @QueryParam("format") @DefaultValue("json") String format) {

		if ("plain".equals(format)) {
			String plainRes = String.format("%s:%s", master.getIp(), master.getPort());
			return Response.status(Status.OK).entity(plainRes).build();
		} else {
			return Response.status(Status.OK).entity(master).build();
		}
	}

	@Path("/{clusterId}/{shardId}/redis/master")
	@POST
	public Response setRedisMaster(@PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
	      @QueryParam("masterIpPort") String masterIpPort) {

		// TODO
		try {
			String[] parts = masterIpPort.split(":");
			if (parts.length == 2) {
				master.setIp(parts[0]);
				master.setPort(Integer.parseInt(parts[1]));
				log.info("change redis master to {}:{}", master.getIp(), master.getPort());
				return Response.status(Status.OK).entity("ok").build();
			}
		} catch (Exception e) {
		}
		return Response.status(Status.OK).entity("error").build();

	}
}
