package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
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

    @RequestMapping(value = "/route", method = RequestMethod.POST)
    public void addRoute(@RequestBody RouteInfoModel model) {
        logger.info("[addRoute] {}", model);
        try {
            routeService.addRoute(model);
        } catch (Throwable th) {
            logger.error("[addRoute]", th);
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.DELETE)
    public void deleteRoute(@RequestBody RouteInfoModel model) {
        logger.info("[deleteRoute] {}", model);

        try {
            routeService.deleteRoute(model.getId());
        } catch (Throwable th) {
            logger.error("[deleteRoute]", th);
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.PUT)
    public void updateRoute(@RequestBody RouteInfoModel model) {
        logger.info("[updateRoute] {}", model);

        try {
            routeService.updateRoute(model);
        } catch (Throwable th) {
            logger.error("[updateRoute]", th);
        }
    }
}
