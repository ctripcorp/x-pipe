package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ReplDirectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ReplDirectionInfoController extends AbstractConsoleController {

    @Autowired
    ReplDirectionService replDirectionService;

    @Autowired
    ClusterService clusterService;


    @RequestMapping("/repl-direction/cluster/" + CLUSTER_NAME_PATH_VARIABLE + "/src-dc/{srcDcName}/to-dc/{toDcName}")
    public ReplDirectionInfoModel getReplDirectionInfoModelByClusterAndSrcToDc(@PathVariable String clusterName,
                                                                               @PathVariable String srcDcName, @PathVariable String toDcName) {
        return replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc(clusterName, srcDcName, toDcName);
    }

    @RequestMapping("/repl-direction/cluster/" + CLUSTER_NAME_PATH_VARIABLE)
    public List<ReplDirectionInfoModel> getAllReplDirectionInfoModelsByCluster(@PathVariable String clusterName) {
        return replDirectionService.findAllReplDirectionInfoModelsByCluster(clusterName);
    }

    @RequestMapping("/repl-direction/infos/all")
    public List<ReplDirectionInfoModel> getAllReplDirectionInfoModels() {
        return replDirectionService.findAllReplDirectionInfoModels();
    }

    @RequestMapping(value = "/repl-direction/repl-completion", method = RequestMethod.POST)
    public RetMessage completeReplicationByReplDirection(@RequestBody ReplDirectionInfoModel replDirection) {
        logger.info("[completeReplicationByReplDirection] {}", replDirection);
        ClusterTbl cluster = clusterService.find(replDirection.getClusterName());
        if (cluster == null) {
            String msg = String.format("cluster %s does not exist", replDirection.getClusterName());
            logger.warn("[completeReplicationByReplDirection] {}", msg);
            return RetMessage.createFailMessage(msg);
        }

        try {
            clusterService.completeReplicationByClusterAndReplDirection(cluster, replDirection);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[completeReplicationByReplDirection] fail {}, {}", replDirection, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }
}
