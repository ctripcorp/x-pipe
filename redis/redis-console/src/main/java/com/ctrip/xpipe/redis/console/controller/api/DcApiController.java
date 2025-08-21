package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcModel;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.spring.AbstractController;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

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

    @RequestMapping(value = "/dc", method = RequestMethod.GET)
    public RetMessage getAllDcs(HttpServletRequest request) {
        logger.info("[DcApiController][getAllDcs]request ip:{}", request.getRemoteHost());

        try {
            List<DcTbl> dcTbls = valueOrEmptySet(DcTbl.class, dcService.findAllDcBasic());
            List<DcModel> dcModels = dcTbls.stream().map(DcModel::fromDcTbl).collect(Collectors.toList());
            return GenericRetMessage.createGenericRetMessage(dcModels);
        } catch (Exception e) {
            logger.error("[DcApiController][getAllDcs]get all dcs exception!", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/dc_tbls", method = RequestMethod.GET)
    public List<DcTbl> getAllDcTbls(HttpServletRequest request) {
        return dcService.findAllDcs();
    }

    @RequestMapping(value = "/dc", method = RequestMethod.PUT)
    public RetMessage updateDc(HttpServletRequest request, @RequestBody DcModel dcModel){

        logger.info("[DcApiController][updateDc]request ip:{}, update dc, dc_name:{}, zoneId:{}, description:{}",request.getRemoteHost(),
                dcModel.getDc_name(), dcModel.getZone_id(), dcModel.getDescription());

        try {
            dcService.updateDcZone(dcModel);
        }catch (Exception e){
            logger.error("[DcApiController][updateDc]update dc exception!", e);
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
                clusterService.updateStatusById(cluster.getId(), ClusterStatus.Normal, 0L);
            }
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/dc/bind", method = RequestMethod.POST)
    public RetMessage bindDc(@RequestBody DcClusterTbl dcClusterTbl){
        try {
            clusterService.bindDc(dcClusterTbl);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

}
