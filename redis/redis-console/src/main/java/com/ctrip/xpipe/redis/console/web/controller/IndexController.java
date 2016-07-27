package com.ctrip.xpipe.redis.console.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;
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
