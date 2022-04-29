package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteDirectionModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.redis.core.entity.Route;
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
        try {
            return routeService.getAllActiveRouteInfoModels();
        } catch (Throwable th) {
            logger.error("[getAllActiveRouteInfos] get all active routes fail", th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/route/id/{routeId}", method = RequestMethod.GET)
    public RouteInfoModel getRouteInfoById(@PathVariable long routeId) {
        return routeService.getRouteInfoModelById(routeId);
    }

    @RequestMapping(value = "/route/src-dc-name/{srcDcName}", method = RequestMethod.GET)
    public List<RouteInfoModel> getAllActiveRoutesByTagAndSrcDcName(@PathVariable String srcDcName) {
        return  routeService.getAllActiveRouteInfoModelsByTagAndSrcDcName(Route.TAG_META, srcDcName);
    }

    @RequestMapping(value = "/route/tag/{tag}/direction/{srcDcName}/{dstDcName}", method = RequestMethod.GET)
    public List<RouteInfoModel> getAllActiveRoutesByTagAndDirection(@PathVariable String tag, @PathVariable String srcDcName, @PathVariable String dstDcName) {
        return  routeService.getAllActiveRouteInfoModelsByTagAndDirection(tag, srcDcName, dstDcName);
    }

    @RequestMapping(value = "/route/tag/{tag}", method = RequestMethod.GET)
    public List<RouteInfoModel> getAllActiveRoutesByTag(@PathVariable String tag) {
        return  routeService.getAllActiveRouteInfoModelsByTag(tag);
    }

    @RequestMapping(value = "/route/direction/tag/{tag}", method = RequestMethod.GET)
    public List<RouteDirectionModel> getAllRouteDirectionInfosByTag(@PathVariable String tag) {
        return routeService.getAllRouteDirectionModelsByTag(tag);
    }

    @RequestMapping(value = "/route", method = RequestMethod.POST)
    public RetMessage addRoute(@RequestBody RouteInfoModel model) {
        try {
            routeService.addRoute(model);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addRoute] add route:{} fail", model, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.DELETE)
    public RetMessage deleteRoute(@RequestBody RouteInfoModel model) {
        try {
            routeService.deleteRoute(model.getId());
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[deleteRoute] delete route:{} fail", model, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/route", method = RequestMethod.PUT)
    public RetMessage updateRoute(@RequestBody RouteInfoModel model) {
        try {
            routeService.updateRoute(model);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRoute] update route:{} fail", model, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/routes", method = RequestMethod.PUT)
    public RetMessage updateRoutes(@RequestBody List<RouteInfoModel> models) {
        try {
            if(!existPublicRouteInfoModel(models)) throw new BadRequestException("none public route in this direction");

            routeService.updateRoutes(models);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRoutes] update routes:{} fail", models, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    private boolean existPublicRouteInfoModel(List<RouteInfoModel> models) {
        for(RouteInfoModel model : models) {
            if (model.isPublic()) return true;
        }
        return false;
    }
}
