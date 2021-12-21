package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzCreateInfo;
import com.ctrip.xpipe.redis.console.service.AzService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

/**
 * @author: Song_Yu
 * @date: 2021/11/8
 */

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class AzUpdateController extends AbstractConsoleController {

    @Autowired
    private AzService azService;

    @RequestMapping(value = "/az", method = RequestMethod.POST)
    public RetMessage addAavilableZone(@RequestBody AzCreateInfo createInfo) {
        try {
            createInfo.check();
            azService.addAvailableZone(createInfo);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addAavilableZone][fail] {}", createInfo, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/az", method = RequestMethod.PUT)
    public RetMessage updateAvailableZone(@RequestBody AzCreateInfo createInfo) {
        try {
            createInfo.check();
            azService.updateAvailableZone(createInfo);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateAvailableZone][fail] {}", createInfo, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/az/{azName:.+}", method = RequestMethod.DELETE)
    public RetMessage delAvailableZoneByName(@PathVariable String azName) {
        try {
            logger.info("[delAvailableZoneByName] {}", azName);
            azService.deleteAvailableZoneByName(azName);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[deleteAvailableZone][fail] {}", azName, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }


    @RequestMapping(value = "/az/{dcName}", method = RequestMethod.GET)
    public List<AzCreateInfo> getAvailableZoneInfosByDc(@PathVariable String dcName) {
        try {
            return azService.getDcAvailableZoneInfos(dcName);
        } catch (Throwable th) {
            logger.error("[getAvailableZoneInfosByDc][fail] {}", dcName, th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/az/all", method = RequestMethod.GET)
    public List<AzCreateInfo> getAllAvailableZoneInfos() {
        try {
            return azService.getAllAvailableZoneInfos();
        } catch (Throwable th) {
            logger.error("[getAllAvailableZoneInfos][fail] {}", th);
            return Collections.emptyList();
        }
    }

}
