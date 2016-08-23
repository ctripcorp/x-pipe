package com.ctrip.xpipe.redis.console.rest.consoleweb;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;

@RestController
@RequestMapping("console")
public class DcController {
	@Autowired
	private DcService dcService;
	
	@RequestMapping(value = "/dcs/all", method = RequestMethod.GET)
	public List<DcTbl> findAllClusters() {
		return dcService.findAllDcs();
	}
}
