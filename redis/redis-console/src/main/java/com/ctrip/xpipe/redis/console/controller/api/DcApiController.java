package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author taotaotu
 * May 23, 2019
 */

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class DcApiController extends AbstractConsoleController {

    @Autowired
    private DcService dcService;

    @RequestMapping(value = "/dc/insert/{zone_id}/{dc_name}/{description}", method = RequestMethod.POST)
    public void addDc(@PathVariable long zone_id, @PathVariable String dc_name, @PathVariable String description){
        dcService.insert(zone_id, dc_name, description);
        logger.info("[DcApiController][addDc]create a new dc, zoneId:{}, dc_name:{}, description:{}",
                zone_id, dc_name, description);
    }

    @RequestMapping(value = "/dc/select/dc_name?{dc_name}", method = RequestMethod.GET)
    public DcTbl queryDcInformationByName(@PathVariable String dc_name){
        return valueOrDefault(DcTbl.class, dcService.find(dc_name));
    }


}
