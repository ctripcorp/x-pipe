package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author zhangle 16/8/24
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class RedisController extends AbstractConsoleController {
    @Autowired
    private RedisService redisService;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private ShardModelService shardModelService;

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public void updateRedises(@PathVariable String clusterName, @PathVariable String dcName,
                              @PathVariable String shardName, @RequestBody(required = false) ShardModel shardModel) {
        try {
            if (null != shardModel) {
                logger.info("[Update Redises][construct]{},{},{},{}", clusterName, dcName, shardName, shardModel);
                redisService.updateRedises(dcName, clusterName, shardName, shardModel);
                logger.info("[Update Redises][success]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            } else {
                logger.error("[Update Redises][Null ShardModel]{},{},{}", clusterName, dcName, shardName);
            }
        } catch (Exception e) {
            logger.error("[Update Redises][failed]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            throw e;
        }
    }

    @RequestMapping(value = "/keepercontainer/migration", method = RequestMethod.POST)
    public void migrateKeepers(@RequestBody MigrationKeeperModel model) {
        logger.info("[migrateKeepers] {}", model);
        if (model.getSrcKeeperContainer() == null || model.getSrcKeeperContainer().getId() == 0) {
            logger.error("[migrateKeepers] src keeperContainer must not be null");
            throw new IllegalArgumentException("src keeperContainer must not be null");
        }

        if (model.getMigrationClusters() == null) {
            model.setMigrationClusters(clusterService.findAllClusterByKeeperContainer(model.getSrcKeeperContainer().getId()));
        }

        int count = 0;
        for (ClusterTbl clusterTbl : model.getMigrationClusters()) {
            List<ShardModel> allShardModel = shardModelService
                    .getAllShardModel(model.getSrcKeeperContainer().getDcName(), clusterTbl.getClusterName());

            for (ShardModel shardModel : allShardModel) {
                if (!shardModelService.migrateBackupKeeper(model.getSrcKeeperContainer().getDcName(),
                        clusterTbl.getClusterName(), shardModel, model.getSrcKeeperContainer().getAddr().getHost(),
                        (model.getTargetKeeperContainer() == null || model.getTargetKeeperContainer().getAddr() == null)
                                ? null : model.getTargetKeeperContainer().getAddr().getHost())) {
                    continue;
                }
                if (model.getMaxMigrationKeeperNum() != 0 && (++count) >= model.getMaxMigrationKeeperNum()) {
                    logger.info("[migrateKeepers] {} rich to max migrate keepers num!", model.getSrcKeeperContainer());
                    return ;
                }
            }
        }
    }
}
