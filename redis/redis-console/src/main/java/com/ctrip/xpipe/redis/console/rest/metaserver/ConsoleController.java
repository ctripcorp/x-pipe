package com.ctrip.xpipe.redis.console.rest.metaserver;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.service.MetaInfoService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author zhangle
 *
 */
@RestController
@RequestMapping("/api")
public class ConsoleController {
	private static Logger logger = LoggerFactory.getLogger(ConsoleController.class);
	private static Codec coder = new JsonCodec();
	
	@Autowired
	private MetaInfoService metaInfoService;
	

	@RequestMapping(value = "/dc/{dcId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcMeta(@PathVariable String dcId, @RequestParam(value="format", required = false) String format) {
		
		DcMeta result;
		try {
			result = metaInfoService.getDcMeta(dcId);
		} catch (DalException e) {
			logger.error(e.getMessage());
			throw new DataNotFoundException(e.getMessage());
		}
		
		return (format != null && format.equals("xml"))? result.toString() : coder.encode(result);
	}
	
	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcClusterMeta(@PathVariable String dcId,@PathVariable String clusterId, @RequestParam(value="format", required = false) String format) {
		
		ClusterMeta result;
		try {
			result = metaInfoService.getDcClusterMeta(dcId, clusterId);
		} catch (DalException e) {
			logger.error(e.getMessage());
			throw new DataNotFoundException(e.getMessage());
		}
		
		return (format != null && format.equals("xml"))? result.toString() : coder.encode(result);
	}
	
	@RequestMapping(value = "/dc/{dcId}/cluster/{clusterId}/shard/{shardId}", method = RequestMethod.GET, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public String getDcClusterShardMeta(@PathVariable String dcId,@PathVariable String clusterId,
			@PathVariable String shardId, @RequestParam(value="format", required = false) String format){
		
		ShardMeta result;
		try {
			result = metaInfoService.getDcClusterShardMeta(dcId, clusterId, shardId);
		} catch (DalException e) {
			logger.error(e.getMessage());
			throw new DataNotFoundException(e.getMessage());
		}
		
		return (format != null && format.equals("xml"))? result.toString() : coder.encode(result);
	}
	
	@RequestMapping(value = "/dcids", method = RequestMethod.GET)
	public List<String> getDcIds(){
		try {
			return metaInfoService.getAllDcIds();
		} catch (DalException e) {
			logger.error(e.getMessage());
			throw new DataNotFoundException(e.getMessage());
		}
	}
	
	@RequestMapping(value = "/clusterids", method = RequestMethod.GET)
	public List<String> getClusterIds(){
		try {
			return metaInfoService.getAllClusterIds();
		} catch (DalException e) {
			logger.error(e.getMessage());
			throw new DataNotFoundException(e.getMessage());
		}
	}
	
	@RequestMapping(value = "/cluster/{clusterId}/shardids", method = RequestMethod.GET)
	public List<String> getShardIds(@PathVariable String clusterId) {
		try {
			return metaInfoService.getAllClusterShardIds(clusterId);
		} catch (DalException e) {
			logger.error(e.getMessage());
			throw new DataNotFoundException(e.getMessage());
		}
	}

}
