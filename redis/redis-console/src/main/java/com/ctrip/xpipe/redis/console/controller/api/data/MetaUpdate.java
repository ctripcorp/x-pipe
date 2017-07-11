package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ShardCreateInfo;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class MetaUpdate extends AbstractConsoleController {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;


    @RequestMapping(value = "/clusters", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage createCluster(@RequestBody ClusterCreateInfo clusterCreateInfo) {

        logger.info("[createCluster]{}", clusterCreateInfo);
        List<DcTbl> dcs = new LinkedList<>();
        try {
            clusterCreateInfo.check();

            ClusterTbl clusterTbl = clusterService.find(clusterCreateInfo.getClusterName());
            if (clusterTbl != null) {
                return RetMessage.createFailMessage("cluster already exist");
            }

            for (String dcName : clusterCreateInfo.getDcs()) {
                DcTbl dcTbl = dcService.find(dcName);
                if (dcTbl == null) {
                    return RetMessage.createFailMessage("dc not exist:" + dcName);
                }
                dcs.add(dcTbl);
            }
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        ClusterModel clusterModel = new ClusterModel();

        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(dcs.get(0).getId())
                .setClusterName(clusterCreateInfo.getClusterName())
                .setClusterDescription(clusterCreateInfo.getDesc())
        );

        clusterModel.setSlaveDcs(dcs.subList(1, dcs.size()));
        clusterService.createCluster(clusterModel);
        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.GET)
    public List<ClusterCreateInfo> getClusters() {

        logger.info("[getClusters]");

        List<ClusterTbl> allClusters = clusterService.findAllClusters();

        List<ClusterCreateInfo> result = new LinkedList<>();
        allClusters.forEach(clusterTbl -> {

            ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
            clusterCreateInfo.setDesc(clusterTbl.getClusterDescription());
            clusterCreateInfo.setClusterName(clusterTbl.getClusterName());

            List<DcTbl> clusterRelatedDc = dcService.findClusterRelatedDc(clusterTbl.getClusterName());
            clusterRelatedDc.forEach(dcTbl -> {

                if(dcTbl.getId() == clusterTbl.getActivedcId()){
                    clusterCreateInfo.addFirstDc(dcTbl.getDcName());
                }else{
                    clusterCreateInfo.addDc(dcTbl.getDcName());
                }
            });

            result.add(clusterCreateInfo);
        });

        return result;
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage createShards(List<ShardCreateInfo> shards) {


        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public List<ShardCreateInfo> getShards(@PathVariable String clusterName) {

        return null;
    }

}
