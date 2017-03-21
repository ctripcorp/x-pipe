package com.ctrip.xpipe.redis.console.controller.pub;

import java.util.LinkedList;
import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.pub.CheckRetMeta.CheckClusterRet;

/**
 * @author wenchao.meng
 *
 * Mar 21, 2017
 */
@RestController
@RequestMapping("/api/migration")
public class MigrationApi extends AbstractConsoleController{
	
	private int ticketId = 1;
	
	@RequestMapping(value = "/checkandprepare", method = RequestMethod.POST, produces={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public CheckRetMeta checkAndPrepare(@RequestBody(required = true) CheckMeta checkMeta) {
		
		logger.info("[checkAndPrepare]{}", checkMeta);
		
		CheckRetMeta checkRetMeta = new CheckRetMeta();
		
		checkRetMeta.setTicketId(ticketId);
		
		List<CheckClusterRet> results = new LinkedList<>();
		results.add(new CheckClusterRet("cluster1", true));
		results.add(new CheckClusterRet("cluster2", false));
		checkRetMeta.setResults(results);
		return checkRetMeta;
	}
	
}
