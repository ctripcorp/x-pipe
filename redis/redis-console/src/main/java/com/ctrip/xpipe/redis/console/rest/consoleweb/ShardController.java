package com.ctrip.xpipe.redis.console.rest.consoleweb;


import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhangle
 */
@RestController
@RequestMapping("console")
public class ShardController {
	@Autowired
	private ClusterMetaService clusterService;
	
	@RequestMapping("/clusters/{clusterName}/dcs/{dcName}/shards")
	public List<ShardMeta> findShards(@PathVariable String clusterName, @PathVariable String dcName) throws DalException {
		return new ArrayList<ShardMeta>(clusterService.getClusterMeta(dcName, clusterName).getShards().values());
	}
}
