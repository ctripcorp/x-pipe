package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class DcClusterUpdateController extends AbstractConsoleController{

    @Autowired
    DcClusterServiceImpl dcClusterService;

    @RequestMapping(value = "/dcCluster", method = RequestMethod.PUT)
    public RetMessage updateRedisCheckRuleOfDcCluster(@RequestBody DcClusterCreateInfo dcClusterCreateInfo) {
        try {
            dcClusterService.updateDcCluster(dcClusterCreateInfo);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRedisCheckRuleOfDcCluster] dcName:{}, clusterName:{}, redisCheckRule:{} fail",
                    dcClusterCreateInfo.getDcName(), dcClusterCreateInfo.getClusterName(), dcClusterCreateInfo.getRedisCheckRule(), th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/dcCluster/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public List<DcClusterCreateInfo> getDcClusterInfoOfCluster(@PathVariable String clusterName) {
        try {
            return dcClusterService.findClusterRelated(clusterName);
        } catch (Throwable th) {
            logger.error("[getDcClusterInfoOfCluster] clusterName:{} fail ", clusterName, th);
            return Collections.emptyList();
        }
    }

}
