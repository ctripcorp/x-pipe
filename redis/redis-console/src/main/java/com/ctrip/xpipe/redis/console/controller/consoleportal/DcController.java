package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcListDcModel;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


/**
 * @author shyin
 *
 * Aug 22, 2016
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class DcController extends AbstractConsoleController{
	@Autowired
	private DcService dcService;

	@RequestMapping(value = "/dcs/all", method = RequestMethod.GET)
	public List<DcTbl> findAllDcs() {
		return valueOrEmptySet(DcTbl.class, dcService.findAllDcBasic());
	}

	@RequestMapping(value = {"/dcs/all/richInformation", "/dcs/all/richInformation/{isCountTypeInHetero}"}, method = RequestMethod.GET)
	public List<DcListDcModel> findAllDcsRichInfo(@PathVariable(required = false) boolean isCountTypeInHetero){
		return valueOrEmptySet(DcListDcModel.class, dcService.findAllDcsRichInfo(isCountTypeInHetero));
	}
}
