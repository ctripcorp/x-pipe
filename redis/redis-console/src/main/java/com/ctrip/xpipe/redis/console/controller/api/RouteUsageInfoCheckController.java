package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.UnexpectedRouteUsageInfoModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class RouteUsageInfoCheckController extends AbstractConsoleController{

    @Autowired
    ClusterService clusterService;

    @RequestMapping(value = "/route/unexpected-usage-infos/all", method = RequestMethod.GET)
    public UnexpectedRouteUsageInfoModel getUnexpectedRouteUsageInfoModel() {
        return clusterService.findUnexpectedRouteUsageInfoModel();
    }

}
