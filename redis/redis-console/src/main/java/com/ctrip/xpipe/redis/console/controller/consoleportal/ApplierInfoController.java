package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ApplierTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.ApplierBasicInfo;
import com.ctrip.xpipe.redis.console.service.ApplierService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.APPLIER_PORT_DEFAULT;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ApplierInfoController extends AbstractConsoleController {

    @Autowired
    ApplierService applierService;

    @Autowired
    ClusterService clusterService;

    @RequestMapping(value = "/dcs/{dcName}/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE
            + "/repl-direction/{replDirectionId}/appliers", method = RequestMethod.POST)
    public RetMessage updateAppliers(@PathVariable String clusterName, @PathVariable String dcName, @PathVariable long replDirectionId,
                             @PathVariable String shardName, @RequestBody(required = false) ShardModel sourceShard) {
        try {
            if (null != sourceShard) {
                logger.info("[Update Appliers][construct]{},{},{},{}", clusterName, dcName, shardName, sourceShard);
                applierService.updateAppliers(dcName, clusterName, shardName, sourceShard, replDirectionId);
                logger.info("[Update Appliers][success]{},{},{},{}", clusterName, dcName, shardName, sourceShard);
            } else {
                logger.error("[Update Appliers][Null SourceShard]{},{},{}", clusterName, dcName, shardName);
            }
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[Update Appliers][failed]{},{},{},{}", clusterName, dcName, shardName, sourceShard);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/dcs/{dcName}/available-appliers", method = RequestMethod.POST)
    public List<ApplierTbl> findAvailableAppliers(@PathVariable String dcName,
                                                  @RequestBody(required = false) ShardModel sourceShard) {

        logger.debug("[findAvailableAppliers]{}, {}", dcName, sourceShard);

        String clusterName = getClusterNameBySourceShard(sourceShard);
        List<ApplierBasicInfo> bestAppliers = applierService.findBestAppliers(dcName, APPLIER_PORT_DEFAULT, (ip, port) -> {
            if (sourceShard != null && isExistOnConsole(ip, port, sourceShard.getAppliers())) {
                return false;
            }
            return true;
        }, clusterName);

        List<ApplierTbl> result = new LinkedList<>();
        bestAppliers.forEach(bestApplier -> {
            result.add(new ApplierTbl().setContainerId(bestApplier.getAppliercontainerId())
                    .setIp(bestApplier.getHost()).setPort(bestApplier.getPort()));
        });

        return result;
    }

    private String getClusterNameBySourceShard(ShardModel sourceShard) {
        logger.debug("[getClusterNameBySourceShard] sourceShard: {}", sourceShard);
        if (sourceShard == null || sourceShard.getShardTbl() == null) return null;

        ClusterTbl clusterTbl = clusterService.find(sourceShard.getShardTbl().getClusterId());
        if (clusterTbl == null) return null;
        return clusterTbl.getClusterName();
    }

    private boolean isExistOnConsole(String ip, int port, List<ApplierTbl> appliers) {
        for (ApplierTbl applier : appliers) {
            if (applier.getIp().equalsIgnoreCase(ip) && applier.getPort() == port) {
                return true;
            }
        }
        return false;
    }
}
