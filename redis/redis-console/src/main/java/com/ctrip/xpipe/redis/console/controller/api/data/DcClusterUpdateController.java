package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterServiceImpl;
import com.ctrip.xpipe.redis.core.entity.Cluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class DcClusterUpdateController extends AbstractConsoleController{

    @Autowired
    DcClusterServiceImpl dcClusterService;

    @Autowired
    ClusterService clusterService;

    @RequestMapping(value = "/dcCluster", method = RequestMethod.PUT)
    public RetMessage updateRedisConfigRuleOfDcCluster(@RequestBody DcClusterCreateInfo dcClusterCreateInfo) {
        try {
            dcClusterService.updateDcCluster(dcClusterCreateInfo);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRedisConfigRuleOfDcCluster] dcName:{}, clusterName:{}, redisConfigRule:{} fail",
                    dcClusterCreateInfo.getDcName(), dcClusterCreateInfo.getClusterName(), dcClusterCreateInfo.getRedisConfigRule(), th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/dcCluster/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public List<DcClusterCreateInfo> getRedisConfigRuleOfCluster(@PathVariable String clusterName) {
        try {
            return dcClusterService.findClusterRelated(clusterName);
        } catch (Throwable th) {
            logger.error("[getRedisConfigRuleOfCluster] clusterName:{} fail ", clusterName, th);
            return Collections.emptyList();
        }
    }

}
