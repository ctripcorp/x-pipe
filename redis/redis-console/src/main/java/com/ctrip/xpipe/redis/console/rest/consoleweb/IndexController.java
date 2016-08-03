package com.ctrip.xpipe.redis.console.rest.consoleweb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;
import com.ctrip.xpipe.redis.console.service.DcService;


/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@RestController("indexController")
public class IndexController {
	
	@Autowired
	private DcService dcService;
	
	@RequestMapping("/api/alldcs")
	public String allDcs() throws DalException {
		return dcService.getAllDcs().toString();
	}
}
