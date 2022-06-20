package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.AzInfoModel;
import com.ctrip.xpipe.redis.console.service.AzService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class AzInfoController extends AbstractConsoleController  {

    @Autowired
    AzService azService;

    @RequestMapping(value = "/az/dcs/{dcId}", method = RequestMethod.GET)
    public List<AzInfoModel> getAllAvailableZoneInfoModelsByDc(@PathVariable long dcId) {
        return azService.getAllAvailableZoneInfoModelsByDc(dcId);
    }
}
