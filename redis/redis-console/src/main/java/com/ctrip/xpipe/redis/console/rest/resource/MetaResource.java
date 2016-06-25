package com.ctrip.xpipe.redis.console.rest.resource;


import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;


@Path("/api/v1")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetaResource extends BaseRecource {
	
	public MetaResource() {
		System.out.println("meta resource");
	}
	
	@Path("/{dc}/{clusterId}/{shardId}/keeper/active")
	@POST
	public Response updateActiveKeeper(@PathParam("dc") String dc, @PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId,
			KeeperMeta activeKeeper) {
		//if activeKeeper is empty, that means all keeper are not active
		
		logger.info("[updateActiveKeeper]{},{},{},{}", dc, clusterId, shardId, activeKeeper);
		getMetaService().updateKeeperActive(dc, clusterId, shardId, activeKeeper);
		return Response.status(Status.OK).build();
	}
	
	@Path("/{dc}/{clusterId}/{shardId}/keeper/active")
	@GET
	public Response updateActiveKeeper(@PathParam("dc") String dc, @PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId) {
		return Response.status(Status.OK).build();
	}
	
}
