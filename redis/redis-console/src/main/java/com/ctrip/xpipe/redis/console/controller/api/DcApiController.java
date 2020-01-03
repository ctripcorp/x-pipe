package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * @author taotaotu
 * May 23, 2019
 */

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class DcApiController extends AbstractConsoleController {

    @Autowired
    private DcService dcService;

    @Autowired
    private ClusterService clusterService;
    
    @RequestMapping(value = "/dc", method = RequestMethod.POST)
    public RetMessage addDc(HttpServletRequest request, @RequestBody DcModel dcModel){

        logger.info("[DcApiController][addDc]request ip:{}, create a new dc, zoneId:{}, dc_name:{}, description:{}",request.getRemoteHost(),
                dcModel.getZone_id(), dcModel.getDc_name(), dcModel.getDescription());

        try {
            dcService.insertWithPartField(dcModel.getZone_id(), dcModel.getDc_name(), dcModel.getDescription());
        }catch (Exception e){
            logger.error("[DcApiController][addDc]add dc exception!", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/debug/reset/clusters/{dcName}", method = RequestMethod.POST)
    public RetMessage resetDcClusters(HttpServletRequest request, @PathVariable String dcName){

        logger.info("[resetDcClusters][addDc]request ip:{}", request.getRemoteHost());

        try {
            List<ClusterTbl> clusters = clusterService.findActiveClustersByDcName(dcName);
            for (ClusterTbl cluster : clusters) {
                if (ClusterStatus.isSameClusterStatus(cluster.getStatus(), ClusterStatus.Normal)) {
                    continue;
                }
                clusterService.updateStatusById(cluster.getId(), ClusterStatus.Normal);
            }
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }

}
