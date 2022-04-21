package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.UnmatchedClusterRouteInfoModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ClusterRouteCheckController extends AbstractConsoleController{

    private JsonCodec pretty = new JsonCodec(true, true);

    private static final Logger logger = LoggerFactory.getLogger(ClusterRouteCheckController.class);

    @Autowired
    ClusterService clusterService;

    @RequestMapping(value = "/cluster/route/unmatched/all", method = RequestMethod.GET)
    public String getUnmatchedClusterRouteInfoModels() {
        logger.info("[getUnmatchedClusterRouteInfoModels]");
        try {
            List<UnmatchedClusterRouteInfoModel> unmatchedClusterRoutes = clusterService.findUnmatchedClusterRoutes();
            return pretty.encode(unmatchedClusterRoutes);
        } catch (Throwable th) {
            logger.error("[getUnmatchedClusterRouteInfoModels]", th);
            return pretty.encode(RetMessage.createFailMessage(th.getMessage()));
        }

    }
}
