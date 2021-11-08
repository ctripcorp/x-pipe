package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.service.AzService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

/**
 * @author:
 * @date:
 */

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class AzUpdateController extends AbstractConsoleController {

    @Autowired
    private AzService azService;

    @RequestMapping(value = "/az", method = RequestMethod.POST)
    public RetMessage addAavilableZone(@RequestBody AzCreateInfo createInfo) {
        try{
            createInfo.check();
            azService.addAvailableZone(createInfo);
            return RetMessage.createSuccessMessage(String.format("add available zone %s successfully", createInfo.getAzName()));
        }catch (Exception e){
            logger.error("[addAavilableZone]" + createInfo);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/az", method = RequestMethod.PUT)
    public RetMessage updateAvailableZone(@RequestBody AzCreateInfo createInfo) {
        try{
            createInfo.check();
            azService.updateAvailableZone(createInfo);
            return RetMessage.createSuccessMessage(String.format("update available zone %s successfully", createInfo.getAzName()));
        }catch (Exception e){
            logger.error("[updateAvailableZone]" + e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/az/{dcName}/{azName:.+}", method = RequestMethod.DELETE)
    public RetMessage delAvailableZoneByName(@PathVariable String dcName, @PathVariable String azName) {
        try {
            logger.info("[Delete Cluster]{}", azName);
            azService.deleteAvailableZoneByName(azName, dcName);
            return RetMessage.createSuccessMessage(String.format("delete available zone %s successfully", azName));
        }catch (Exception e){
            logger.error("[deleteAvailableZone]" + e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }


    @RequestMapping(value = "/az/{dcName}", method = RequestMethod.GET)
    public List<AzCreateInfo> getAvailableZoneByDc(@PathVariable String dcName) {
        try {
            return azService.getDcAvailableZones(dcName);
        }catch (Exception e){
            logger.error("[]", e);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/az/all", method = RequestMethod.GET)
    public List<AzCreateInfo> getAllAvailableZone() {
        try {
            return azService.getAllAvailableZones();
        }catch (Exception e){
            logger.error("[]", e);
            return Collections.emptyList();
        }
    }

}
