package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author taotaotu
 * May 23, 2019
 */

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ZoneController extends AbstractConsoleController {

    @Autowired
    private ZoneService zoneService;

    @RequestMapping(value = "/zone/select/zone_id?{zone_id}", method = RequestMethod.GET)
    public ZoneTbl findZoneById(@PathVariable long zone_id){
        return valueOrDefault(ZoneTbl.class, zoneService.findById(zone_id));
    }

    @RequestMapping(value = "/zone/select/all", method = RequestMethod.GET)
    public List<ZoneTbl> findAllZones(){
        return valueOrEmptySet(ZoneTbl.class, zoneService.findAllZones());
    }

    @RequestMapping(value = "/zone/insert/{zone_name}", method = RequestMethod.POST)
    public void addZone(@PathVariable String zone_name){
        zoneService.insertRecord(zone_name);
        logger.info("[ZoneController][addZone]create new zone, zoneName:{}", zone_name);
    }
}
