package com.ctrip.xpipe.redis.console.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Readset;

import com.ctrip.xpipe.redis.console.web.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.web.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTblEntity;
import com.ctrip.xpipe.redis.console.web.service.DcClusterShardService;

@RestController("indexController")
public class IndexController {
	
	@Autowired
	private DcClusterShardService dcClusterShardService;
	
	@RequestMapping("/getcluster")
	public String xpipeConsoleIndex() throws DalException {
		return dcClusterShardService.getByPK("pk").toString();
	}
}
