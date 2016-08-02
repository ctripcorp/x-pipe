package com.ctrip.xpipe.redis.meta.server.rest.impl;




import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.rest.MetaApi;


@RestController
@RequestMapping("/api/v1")
public class MetaResource extends AbstractRecource implements MetaApi{
	
	@Autowired
	private MetaServer metaServer;
	
	public static String FAKE_ADDRESS = "0.0.0.0:0";
	
	public MetaResource() {
	}

	
	@RequestMapping(path = "/{clusterId}/{shardId}", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public ShardStatus getClusterStatus(@PathVariable final String clusterId, @PathVariable final String shardId,
			@RequestParam(name = "version", defaultValue = "0") long version) throws Exception {
		return metaServer.getShardStatus(clusterId, shardId);
	}

	@RequestMapping( path = "/{clusterId}/{shardId}/keeper/master", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public Object getActiveKeeper(@PathVariable String clusterId, @PathVariable String shardId,
			@RequestParam(name = "version", defaultValue = "0") long version, @RequestParam(name = "format", defaultValue = "json")String format) {
		
		KeeperMeta keeper = doGetActiveKeeper(clusterId, shardId);

		if (keeper == null) {
			return FAKE_ADDRESS;
		} else {
			if ("plain".equals(format)) {
				String plainRes = String.format("%s:%s", keeper.getIp(), keeper.getPort());
				return plainRes;
			} else {
				return keeper;
			}
		}
	}

	@RequestMapping(path = "/{clusterId}/{shardId}/keeper/upstream", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public KeeperMeta getUpstreamKeeper(@PathVariable final String clusterId, @PathVariable final String shardId) throws Exception {
		
		return doGetUpstreamKeeper(clusterId, shardId);
	}

	@RequestMapping(path = "/{clusterId}/{shardId}/redis/master", method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public Object getRedisMaster(@PathVariable String clusterId, @PathVariable String shardId,
			@RequestParam( name = "version", defaultValue="0") long version, @RequestParam(name = "format", defaultValue = "json") String format) {

		RedisMeta master = doGetRedisMaster(clusterId, shardId);

		if ("plain".equals(format)) {
			if (master == null) {
				return FAKE_ADDRESS;
			}
			String plainRes = String.format("%s:%s", master.getIp(), master.getPort());
			return plainRes;
		} else {
			return master;
		}
	}

	@RequestMapping( path = "/promote/{clusterId}/{shardId}/{ip}/{port}", method = RequestMethod.POST, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public void promoteRedisMaster(@PathVariable final String clusterId, @PathVariable final String shardId, @PathVariable("ip") final String promoteIp, @PathVariable("port") final int promotePort) throws Exception {

		logger.info("[promoteRedisMaster]{},{},{}:{}", clusterId , shardId , promoteIp , promotePort);

		metaServer.promoteRedisMaster(clusterId, shardId, promoteIp, promotePort);
	}
	
	@RequestMapping( path = "/setupstream/{clusterId}/{shardId}/{ip}/{port}", method = RequestMethod.POST)
	public void setUpstream(@PathVariable final String clusterId, @PathVariable final String shardId, 
			@RequestParam("ip") final String upstreamIp, @RequestParam("port") final int upstreamPort) throws Exception {

		logger.info("[setUpstream]{},{},{}:{}", clusterId , shardId , upstreamIp, upstreamPort);
		metaServer.updateUpstream(clusterId, shardId, String.format("%s:%p", upstreamIp, upstreamPort));

	}

	private KeeperMeta doGetActiveKeeper(String clusterId, String shardId) {
		KeeperMeta keeper = metaServer.getActiveKeeper(clusterId, shardId);
		return keeper;
	}

	private KeeperMeta doGetUpstreamKeeper(String clusterId, String shardId) throws Exception {
		return metaServer.getUpstreamKeeper(clusterId, shardId);
	}

	private RedisMeta doGetRedisMaster(String clusterId, String shardId) {
		return metaServer.getRedisMaster(clusterId, shardId);
	}

}