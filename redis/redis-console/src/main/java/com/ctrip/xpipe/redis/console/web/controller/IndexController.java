package com.ctrip.xpipe.redis.console.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;
import com.ctrip.xpipe.redis.console.web.service.DcService;


/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@RestController("indexController")
public class IndexController {
	
	@Autowired
	private DcService dcClusterShardService;
	
	@RequestMapping("/api/alldcs")
	public String getAllDc() throws DalException {
		return dcClusterShardService.getAllDcs().toString();
	}
}