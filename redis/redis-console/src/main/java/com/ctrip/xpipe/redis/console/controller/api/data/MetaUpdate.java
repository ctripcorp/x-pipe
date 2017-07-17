package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ShardCreateInfo;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    @Autowired
    private SentinelService sentinelService;

    @Autowired
    private ShardService shardService;


    @RequestMapping(value = "/clusters", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage createCluster(@RequestBody ClusterCreateInfo outerClusterCreateInfo) {

        ClusterCreateInfo clusterCreateInfo = transform(outerClusterCreateInfo);

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

    private ClusterCreateInfo transform(ClusterCreateInfo outerClusterCreateInfo) {
        List<String> dcs = outerClusterCreateInfo.getDcs();

        List<String> trans = new LinkedList<>();

        for(String dc : dcs){
            String inner = outerDcToInnerDc(dc);
            if(!Objects.equals(inner, dc)){
                logger.info("[transform]{}->{}", dc, inner);
            }
            trans.add(inner);
        }

        outerClusterCreateInfo.setDcs(trans);
        return outerClusterCreateInfo;
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

                if (dcTbl.getId() == clusterTbl.getActivedcId()) {
                    clusterCreateInfo.addFirstDc(dcTbl.getDcName());
                } else {
                    clusterCreateInfo.addDc(dcTbl.getDcName());
                }
            });

            result.add(clusterCreateInfo);
        });

        return result;
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage createShards(@PathVariable String clusterName, @RequestBody List<ShardCreateInfo> shards) {

        logger.info("[createShards]{}, {}", clusterName, shards);

        ClusterTbl clusterTbl  = null;

        try {
            clusterTbl = clusterService.find(clusterName);
            if (clusterTbl == null) {
                return RetMessage.createFailMessage("cluster not exist");
            }
            for (ShardCreateInfo shardCreateInfo : shards) {
                shardCreateInfo.check();
            }
        } catch (CheckFailException e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        Map<Long, SetinelTbl> randomSentinelByDc = sentinelService.eachRandomSentinelByDc();
        List<String> successShards = new LinkedList<>();
        List<String> failShards = new LinkedList<>();

        for(ShardCreateInfo shardCreateInfo : shards){

            try{
                ShardTbl shardTbl = new ShardTbl()
                        .setSetinelMonitorName(shardCreateInfo.getShardMonitorName())
                        .setShardName(shardCreateInfo.getShardName());
                shardService.createShard(clusterName, shardTbl, randomSentinelByDc);
                successShards.add(shardCreateInfo.getShardName());
            }catch (Exception e){
                failShards.add(shardCreateInfo.getShardName());
            }
        }
        if(failShards.size() == 0){
            return RetMessage.createSuccessMessage();
        }else{
            return RetMessage.createFailMessage(String.format("success shards:%s, fail shards:", joiner.join(successShards), joiner.join(failShards)));
        }
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public List<ShardCreateInfo> getShards(@PathVariable String clusterName) {

        List<ShardTbl> allByClusterName = shardService.findAllByClusterName(clusterName);
        List<ShardCreateInfo> result = new LinkedList<>();

        allByClusterName.forEach(shardTbl -> result.add(new ShardCreateInfo(shardTbl.getShardName(), shardTbl.getSetinelMonitorName())));
        return result;
    }

}
