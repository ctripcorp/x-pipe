package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerMigrationService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeeperContainerInfoController extends AbstractConsoleController {

    @Autowired
    KeeperContainerService keeperContainerService;

    @Autowired
    KeeperContainerUsedInfoAnalyzer analyzer;

    @Autowired
    KeeperContainerMigrationService keeperContainerMigrationService;

    @RequestMapping(value = "/keepercontainer/infos/all", method = RequestMethod.GET)
    public List<KeeperContainerInfoModel> getAllKeeperContainerInfos() {
        return keeperContainerService.findAllInfos();
    }


    @RequestMapping(value = "/keepercontainer/{id}", method = RequestMethod.GET)
    public KeeperContainerInfoModel getKeeperContainerById(@PathVariable long id) {
        return keeperContainerService.findKeeperContainerInfoModelById(id);
    }

    @RequestMapping(value = "/keepercontainer", method = RequestMethod.POST)
    public RetMessage addKeeperContainer(@RequestBody KeeperContainerInfoModel keeperContainer) {
        logger.info("[addKeeperContainer]{}", keeperContainer);
        try {
            keeperContainerService.addKeeperContainerByInfoModel(keeperContainer);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addKeeperContainer] {} fail", keeperContainer, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/keepercontainer", method = RequestMethod.PUT)
    public RetMessage updateKeeperContainer(@RequestBody KeeperContainerInfoModel keeperContainer) {
        logger.info("[updateKeeperContainer]{}", keeperContainer);
        try {
            keeperContainerService.updateKeeperContainerByInfoModel(keeperContainer);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateKeeperContainer] {} fail", keeperContainer, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = {"/keepercontainers/dc/{dcName}/az/{azName}/org/{orgName}",
                            "/keepercontainers/dc/{dcName}/az/{azName}/org",
                            "/keepercontainers/dc/{dcName}/az/org/{orgName}",
                            "/keepercontainers/dc/{dcName}/az/org"},
                    method = RequestMethod.GET)
    public List<KeeperContainerInfoModel> getAvailableKeeperContainersByDcAzAndOrg(@PathVariable String dcName,
                        @PathVariable(required = false) String azName, @PathVariable(required = false) String orgName) {
        return keeperContainerService.findAvailableKeeperContainerInfoModelsByDcAzAndOrg(dcName, azName, orgName);
    }

    @RequestMapping(value = "/keepercontainers/overload/all", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getAllOverloadKeeperContainers() {
        return analyzer.getAllReadyToMigrationKeeperContainers();
    }


    @RequestMapping(value = "/keepercontainer/overload/migration/process", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getOverloadKeeperContainerMigrationProcess() {
        return keeperContainerMigrationService.getMigrationProcess();
    }

    @RequestMapping(value = "/keepercontainer/overload/migration/begin", method = RequestMethod.POST)
    public RetMessage beginToMigrateOverloadKeeperContainers(@RequestBody List<MigrationKeeperContainerDetailModel> keeperContainerDetailModels) {
        logger.info("begin to migrate over load keeper containers {}", keeperContainerDetailModels);
        try {
            keeperContainerMigrationService.beginMigrateKeeperContainers(keeperContainerDetailModels);
        } catch (Throwable th) {
            logger.warn("migrate over load keeper containers {} fail by {}", keeperContainerDetailModels, th.getMessage());
            return RetMessage.createFailMessage(th.getMessage());
        }
        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/keepercontainer/overload/info/lasted", method = RequestMethod.GET)
    public List<KeeperContainerUsedInfoModel>  getLastedAllReadyMigrateKeeperContainers() {
        return analyzer.getAllKeeperContainerUsedInfoModelsList();
    }

    @RequestMapping(value = "/keepercontainer/max/fullSynchronizationTime", method = RequestMethod.GET)
    public RetMessage getMaxKeeperContainerFullSynchronizationTime() {
        return RetMessage.createSuccessMessage(String.valueOf(analyzer.getMaxKeeperContainerFullSynchronizationTime() + 1));
    }

}
