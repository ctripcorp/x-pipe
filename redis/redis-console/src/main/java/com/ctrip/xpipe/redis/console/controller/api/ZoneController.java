package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ZoneModel;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import com.ctrip.xpipe.spring.AbstractController;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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

    @RequestMapping(value = "/zone/{zoneId}", method = RequestMethod.GET)
    public ZoneTbl findZoneById(@PathVariable long zoneId){
        return valueOrDefault(ZoneTbl.class, zoneService.findById(zoneId));
    }

    @RequestMapping(value = "/zone/all", method = RequestMethod.GET)
    public List<ZoneTbl> findAllZones(){
        return valueOrEmptySet(ZoneTbl.class, zoneService.findAllZones());
    }

    @RequestMapping(value = "/zone", method = RequestMethod.POST)
    public RetMessage addZone(HttpServletRequest request, @RequestBody ZoneModel zoneModel){
        logger.info("[ZoneController][addZone]remote ip:{}, zoneName: {}", request.getRemoteHost(), zoneModel.getName());

        try{
            zoneService.insertRecord(zoneModel.getName());
        }catch (Exception e){
            logger.error("[ZoneController][addZone]add zone exception", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }
}
