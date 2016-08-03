package com.ctrip.xpipe.redis.console.rest.consoleweb;

import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClusterController {

	@RequestMapping("/clusters/{clusterName}")
	public ClusterVO loadCluster(@PathVariable String clusterName){
		return null;

	}

}
