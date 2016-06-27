package com.ctrip.xpipe.redis.console.rest.resource;


import java.util.concurrent.Callable;

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
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;


@Path("/api/v1")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetaResource extends BaseRecource {
	
	public MetaResource() {
	}
	
	@Path("/{dc}/{clusterId}/{shardId}/keeper/active")
	@POST
	public Response updateActiveKeeper(@PathParam("dc") final String dc, @PathParam("clusterId") final String clusterId, @PathParam("shardId") final String shardId,
			final KeeperMeta activeKeeper) {
		
		logger.info("[updateActiveKeeper]{},{},{},{}", dc, clusterId, shardId, activeKeeper);
		return template.process(new Callable<Response>() {
			
			@Override
			public Response call() throws Exception {
				getMetaService().updateKeeperActive(dc, clusterId, shardId, activeKeeper);
				return Response.status(Status.OK).build();
			}
		});
	}

	@Path("/{dc}/{clusterId}/{shardId}/redis/master")
	@POST
	public Response updateRedisMaster(@PathParam("dc") final String dc, @PathParam("clusterId") final String clusterId, @PathParam("shardId") final String shardId,
			final RedisMeta redisMaster) {
		logger.info("[updateRedisMaster]{},{},{},{}", dc, clusterId, shardId, redisMaster);
		
		return template.process(new Callable<Response>() {
			@Override
			public Response call() throws Exception {
				getMetaService().updateRedisMaster(dc, clusterId, shardId, redisMaster);
				return Response.status(Status.OK).build();
			}
		});
	}

	@Path("/xpipe")
	@GET
	public Response getXpipe(@PathParam("dc") String dc, @PathParam("clusterId") String clusterId, @PathParam("shardId") String shardId) {
		
		XpipeMeta xpipeMeta = getMetaDao().getXpipeMeta();
		return Response.status(Status.OK).entity(xpipeMeta).build();
	}
	
}
