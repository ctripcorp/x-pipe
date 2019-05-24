package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ZoneModel;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
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

    @RequestMapping(value = "/zone/{zone_id}", method = RequestMethod.GET)
    public ZoneTbl findZoneById(@PathVariable long zone_id){
        return valueOrDefault(ZoneTbl.class, zoneService.findById(zone_id));
    }

    @RequestMapping(value = "/zone/all", method = RequestMethod.GET)
    public List<ZoneTbl> findAllZones(){
        return valueOrEmptySet(ZoneTbl.class, zoneService.findAllZones());
    }

    @RequestMapping(value = "/zone", method = RequestMethod.POST)
    public RetMessage addZone(HttpServletRequest request, @RequestBody ZoneModel zoneModel){
        logger.info("[ZoneController][addZone]remote ip:{}, zone_name: {}", request.getRemoteHost(), zoneModel.getName());

        try{
            zoneService.insertRecord(zoneModel.getName());
        }catch (Exception e){
            logger.error("[ZoneController][addZone]add zone exception", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }
}
