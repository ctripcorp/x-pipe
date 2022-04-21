package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.UnmatchedClusterRouteInfoModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ClusterRouteCheckController extends AbstractConsoleController{

    @Autowired
    ClusterService clusterService;

    @RequestMapping(value = "/cluster/route/unmatched/all", method = RequestMethod.GET)
    public List<UnmatchedClusterRouteInfoModel> getUnmatchedClusterRouteInfoModels() {
        return clusterService.findUnmatchedClusterRoutes();
    }
}
