package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.UseWrongRouteClusterInfoModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class WrongRouteUsedCheckController extends AbstractConsoleController{

    private static final Logger logger = LoggerFactory.getLogger(WrongRouteUsedCheckController.class);

    @Autowired
    ClusterService clusterService;

    @RequestMapping(value = "/wrong/route/used/all", method = RequestMethod.GET)
    public UseWrongRouteClusterInfoModel getWrongRouteUsedInfoModel() {
        logger.info("[getWrongRouteUsedInfoModel]");
        return clusterService.findUseWrongRouteClusterInfos();
    }
}
