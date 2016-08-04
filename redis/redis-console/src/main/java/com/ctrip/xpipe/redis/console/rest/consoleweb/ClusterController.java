package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO;
import com.ctrip.xpipe.redis.console.service.MetaInfoService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;

@RestController
public class ClusterController {
	
	@Autowired
	private MetaInfoService metaInfoService;

	@RequestMapping("/clusters/{clusterName}")
	public ClusterVO loadCluster(@PathVariable String clusterName) throws DalException{
		
		return metaInfoService.getClusterVO(clusterName);
	}

}
