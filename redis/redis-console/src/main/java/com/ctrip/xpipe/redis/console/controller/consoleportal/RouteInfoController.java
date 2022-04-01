package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteDirectionModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModels;
import com.ctrip.xpipe.redis.console.service.RouteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class RouteInfoController extends AbstractConsoleController {

    @Autowired
    private RouteService routeService;

    @RequestMapping(value = "/route/status/all", method = RequestMethod.GET)
    public List<RouteInfoModel> getAllActiveRouteInfos() {
        logger.info("[getAllActiveRouteInfos]");
        try {
            List<RouteInfoModel> routeInfoModels =  routeService.getAllActiveRouteInfos();
            return routeInfoModels;
        } catch (Throwable th) {
            logger.error("[getAllActiveRouteInfos]", th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/route/id/{routeId}", method = RequestMethod.GET)
    public RouteInfoModel getRouteInfoById(@PathVariable long routeId) {
        logger.info("[getRouteInfoById]");
        try {
            return routeService.getRouteInfoById(routeId);
        } catch (Throwable th) {
            logger.error("[getRouteInfoById]", th);
            return null;
        }
    }

    @RequestMapping(value = "/route/tag/{tag}/direction/{srcDcName}/{dstDcName}", method = RequestMethod.GET)
    public List<RouteInfoModel> getAllActiveRoutesByTagAndDirection(@PathVariable String tag, @PathVariable String srcDcName, @PathVariable String dstDcName) {
        logger.info("[getAllActiveRoutesByTagAndDirection]srcDcName:{}, dstDcName:{}, tag:{}",srcDcName, dstDcName, tag);
        try {
            return  routeService.getAllRouteInfosByTagAndDirection(tag, srcDcName, dstDcName);
        } catch (Throwable th) {
            logger.error("[getAllActiveRoutesByTagAndDirection]srcDcName:{}, dstDcName:{}, tag:{}",srcDcName, dstDcName, tag, th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/route/tag/{tag}", method = RequestMethod.GET)
    public List<RouteInfoModel> getAllActiveRoutesByTag(@PathVariable String tag) {
        logger.info("[getAllActiveRoutesByTag]：{}", tag);
        try {
            return  routeService.getAllActiveRouteInfosByTag(tag);
        } catch (Throwable th) {
            logger.error("[getAllActiveRoutesByTag]", th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/route/direction/tag/{tag}", method = RequestMethod.GET)
    public List<RouteDirectionModel> getAllRouteDirectionInfosByTag(@PathVariable String tag) {
        logger.info("[getAllRouteDirectionsByTag]：{}", tag);
        try {
            return  routeService.getRouteDirectionModesByTag(tag);
        } catch (Throwable th) {
            logger.error("[getAllRouteDirectionsByTag]", th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.POST)
    public RetMessage addRoute(@RequestBody RouteInfoModel model) {
        logger.info("[addRoute] {}", model);
        try {
            routeService.addRoute(model);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addRoute]", th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.DELETE)
    public RetMessage deleteRoute(@RequestBody RouteInfoModel model) {
        logger.info("[deleteRoute] {}", model);

        try {
            routeService.deleteRoute(model.getId());
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[deleteRoute]", th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.PUT)
    public RetMessage updateRoute(@RequestBody RouteInfoModel model) {
        logger.info("[updateRoute] {}", model);

        try {
            routeService.updateRoute(model);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRoute]", th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/routes", method = RequestMethod.PUT)
    public RetMessage updateRoutes(@RequestBody RouteInfoModels model) {
        logger.info("[updateRoutes] {}", model.getRouteInfoModels());

        try {
            routeService.updateRoutes(model.getRouteInfoModels());
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRoutes]", th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }
}
