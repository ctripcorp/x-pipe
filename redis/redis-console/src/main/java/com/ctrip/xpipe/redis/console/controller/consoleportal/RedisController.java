package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;

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
    @Autowired
    private KeeperAdvancedService keeperAdvancedService;
    @Autowired
    private KeeperContainerService keeperContainerService;

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
                if (!migrateShardKeepers(clusterTbl.getClusterName(), model, shardModel)) {
                    continue;
                }
                if (model.getMaxMigrationKeeperNum() != 0 && (++count) >= model.getMaxMigrationKeeperNum()) {
                    logger.info("[migrateKeepers] {} rich to max migrate keepers num!", model.getSrcKeeperContainer());
                    return ;
                }
            }
        }
    }

    private boolean migrateShardKeepers(String clusterName, MigrationKeeperModel model, ShardModel shardModel) {
        List<RedisTbl> newKeepers =
                getNewKeepers(clusterName, shardModel, model.getSrcKeeperContainer(), model.getTargetKeeperContainer());

        if (newKeepers == null) {
            logger.debug("[migrateKeepers] no need to replace keepers");
            return false;
        }else if (newKeepers.size() == 2) {
            return doMigrateKeepers(shardModel, newKeepers, clusterName,
                            model.getSrcKeeperContainer().getDcName(), shardModel.getShardTbl().getShardName());
        } else {
            logger.info("[migrateKeepers] fail to migrate keepers with unexpected newKeepers {}", newKeepers);
            return false;
        }
    }

    private List<RedisTbl> getNewKeepers(String clusterName, ShardModel shardModel,
                         KeeperContainerInfoModel srcKeeperContainer, KeeperContainerInfoModel targetKeeperContainer) {
        List<RedisTbl> newKeepers = new ArrayList<>();
        for (RedisTbl keeper : shardModel.getKeepers()) {
            if (!ObjectUtils.equals(keeper.getKeepercontainerId(), srcKeeperContainer.getId())) {
                newKeepers.add(keeper);
            }
        }

        if (newKeepers.size() == 2) {
            return null;
        }

        if (newKeepers.size() < 1) {
            logger.info("[migrateKeepers] unexpected keepers {} from cluster:{}, dc:{}, shard:{}",
                    newKeepers, clusterName, srcKeeperContainer.getDcName(), shardModel.getShardTbl().getShardName());
            return newKeepers;
        }

        long alreadyUsedAzId = keeperContainerService.find(newKeepers.get(0).getKeepercontainerId()).getAzId();
        List<KeeperBasicInfo> bestKeepers = findBestKeepers(clusterName, srcKeeperContainer.getDcName(), targetKeeperContainer);
        for (KeeperBasicInfo keeperSelected : bestKeepers) {
            if (keeperSelected.getKeeperContainerId() != srcKeeperContainer.getId()
                    && keeperSelected.getKeeperContainerId() != newKeepers.get(0).getKeepercontainerId()
                    && isDifferentAz(keeperSelected, alreadyUsedAzId)) {
                newKeepers.add(new RedisTbl().setKeepercontainerId(keeperSelected.getKeeperContainerId())
                        .setRedisIp(keeperSelected.getHost())
                        .setRedisPort(keeperSelected.getPort())
                        .setRedisRole(XPipeConsoleConstant.ROLE_KEEPER));
                break;
            }
        }
        return newKeepers;
    }

    private boolean isDifferentAz(KeeperBasicInfo keeperSelected, long alreadyUsedAzId) {
        if (alreadyUsedAzId == 0)  return true;
        long newAzId = keeperContainerService.find(keeperSelected.getKeeperContainerId()).getAzId();
        return newAzId != alreadyUsedAzId;
    }

    private boolean doMigrateKeepers(ShardModel shardModel, List<RedisTbl> newKeepers, String clusterName,
                                     String dcName, String shardName) {
        try {
            shardModel.setKeepers(newKeepers);
            logger.info("[Update Redises][construct]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            redisService.updateRedises(dcName, clusterName, shardName, shardModel);
            logger.info("[Update Redises][success]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            return true;
        } catch (Exception e) {
            logger.error("[Update Redises][failed]{},{},{},{}", clusterName, dcName, shardName, shardModel, e);
            return false;
        }
    }

    private List<KeeperBasicInfo> findBestKeepers(String clusterName, String dcName, KeeperContainerInfoModel targetKeeperContainer) {
        if (targetKeeperContainer == null || targetKeeperContainer.getId() == 0) {
            return keeperAdvancedService.findBestKeepers(dcName, KEEPER_PORT_DEFAULT, (ip, port) -> true, clusterName);
        } else {
            return keeperAdvancedService.findBestKeepersByKeeperContainer(targetKeeperContainer, KEEPER_PORT_DEFAULT, (ip, port) -> true, 1);
        }
    }
}
