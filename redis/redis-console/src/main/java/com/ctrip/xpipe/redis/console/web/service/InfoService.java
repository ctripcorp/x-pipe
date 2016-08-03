package com.ctrip.xpipe.redis.console.web.service;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ctrip.xpipe.redis.console.web.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcTblDao;
import com.ctrip.xpipe.redis.console.web.model.ShardTblDao;

/**
 * @author shyin
 *
 * Aug 3, 2016
 */

@Service("infoService")
public class InfoService {
	
	private Logger logger = LoggerFactory.getLogger(InfoService.class);
	
	@Autowired
	private DcTblDao dcTblDao;
	
	@Autowired
	private ClusterTblDao clusterTblDao;
	
	@Autowired
	private DcClusterTblDao dcClusterTblDao;
	
	@Autowired 
	private ShardTblDao shardTblDao;
	
	@Autowired
	private DcClusterShardTblDao dcClusterShardTblDao;
	
	
	/**
	 * @return 
	 */
	public Set<String> getAllDcIds() {
		return null;
	}
	
	public Set<String> getAllClusterIds() {
		return null;
	}
	
	public Set<String> getClusterShardIds() {
		return null;
	}
}
