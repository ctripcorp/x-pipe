package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class RouteController extends AbstractConsoleController {
    @Autowired
    private RouteService routeService;

    @RequestMapping(value = "/exist/route/active/{activeDc}/backup/{backupDc}", method = RequestMethod.GET)
    public RetMessage existRoutes(@PathVariable String activeDc, @PathVariable String backupDc) {
        logger.info("[existRoutes] {}, {}", activeDc, backupDc);
        if(StringUtil.trimEquals(activeDc, backupDc, true)) {
            return RetMessage.createFailMessage("false");
        }
        if(routeService.existsRouteBetweenDc(activeDc, backupDc)) {
            return RetMessage.createSuccessMessage();
        }
        return RetMessage.createFailMessage("false");
    }

    @RequestMapping(value = "/exist/route/dc/{currentDc}/cluster/{clusterId}", method = RequestMethod.GET)
    public RetMessage existPeerRoutes(@PathVariable String currentDc, @PathVariable String clusterId) {
        logger.info("[existRoutes] {}, {}", currentDc, clusterId);
        if(routeService.existPeerRoutes(currentDc, clusterId)) {
            return RetMessage.createSuccessMessage();
        }
        return RetMessage.createFailMessage("false");
    }
}
