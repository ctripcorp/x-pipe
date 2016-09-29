package com.ctrip.xpipe.redis.console.rest.consoleweb;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;

/**
 * @author shyin
 *
 * Aug 22, 2016
 */
@RestController
@RequestMapping("console")
public class DcController extends AbstractConsoleController{
	@Autowired
	private DcService dcService;
	@Autowired
	private KeepercontainerService keepercontainerService;
	
	@RequestMapping(value = "/dcs/all", method = RequestMethod.GET)
	public List<DcTbl> findAllDcs() {
		return valueOrEmptySet(DcTbl.class, dcService.findAllDcBasic());
	}

	@RequestMapping(value = "/dcs/{dcName}/keepercontainers", method = RequestMethod.GET)
	public List<KeepercontainerTbl> findKeeperContainer(@PathVariable String dcName){
		return keepercontainerService.findByDcName(dcName);
	}
}
