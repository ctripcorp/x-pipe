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

import com.ctrip.xpipe.redis.keeper.entity.Keeper;
import com.ctrip.xpipe.redis.metaserver.MetaServer;

@Path("/api/v1")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource extends BaseRecource {

	@Path("/{clusterId}/{shardId}")
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
}
