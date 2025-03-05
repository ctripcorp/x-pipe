package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.entity.CheckerReportSituation;
import com.ctrip.xpipe.redis.console.keeper.entity.KeeperContainerDiskType;
import com.ctrip.xpipe.redis.console.keeper.impl.KeeperContainerUsedInfoMsgCollector;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.KeeperMsgModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerMigrationService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_CONTAINER_STANDARD;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeeperContainerInfoController extends AbstractConsoleController {

    @Autowired
    KeeperContainerService keeperContainerService;

    @Autowired
    KeeperContainerUsedInfoAnalyzer analyzer;

    @Autowired
    KeeperContainerMigrationService keeperContainerMigrationService;

    @Autowired
    KeeperContainerUsedInfoMsgCollector keeperContainerUsedInfoMsgCollector;

    @Autowired
    ConfigService configService;

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

    @RequestMapping(value = "/keepercontainers", method = RequestMethod.GET)
    public List<KeeperContainerInfoModel> getAvailableKeeperContainersByDcAzAndOrg(
            @RequestParam String dcName,
            @RequestParam(required = false) String azName,
            @RequestParam(required = false) String orgName,
            @RequestParam(required = false) String tag) {
        return keeperContainerService.findAvailableKeeperContainerInfoModelsByDcAzOrgAndTag(dcName, azName, orgName, tag);
    }

    @RequestMapping(value = "/keepercontainers/overload/all", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getAllOverloadKeeperContainers() {
        return analyzer.getAllDcReadyToMigrationKeeperContainers();
    }


    @RequestMapping(value = "/keepercontainer/overload/migration/process", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getOverloadKeeperContainerMigrationProcess() {
        return keeperContainerMigrationService.getMigrationProcess();
    }

    @RequestMapping(value = "/keepercontainer/overload/migration/begin", method = RequestMethod.POST)
    public RetMessage beginToMigrateOverloadKeeperContainers(@RequestBody List<MigrationKeeperContainerDetailModel> keeperContainerDetailModels) {
        try {
            if (!keeperContainerMigrationService.beginMigrateKeeperContainers(keeperContainerDetailModels)) {
                return RetMessage.createFailMessage("The previous migration tasks are still in progress!");
            }
        } catch (Throwable th) {
            logger.warn("[beginToMigrateOverloadKeeperContainers][fail] {}", keeperContainerDetailModels, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/keepercontainer/overload/migration/terminate", method = RequestMethod.POST)
    public RetMessage migrateKeeperTaskTerminate() {
        if(keeperContainerMigrationService.stopMigrate()){
            return RetMessage.createSuccessMessage("All migration tasks have been completed");
        }
        return RetMessage.createSuccessMessage("No migration tasks in progress");
    }

    @RequestMapping(value = "/keepercontainer/overload/info/lasted", method = RequestMethod.GET)
    public List<KeeperContainerUsedInfoModel>  getLastedAllReadyMigrateKeeperContainers() {
        return analyzer.getAllDcKeeperContainerUsedInfoModelsList();
    }

    @RequestMapping(value = "/keepercontainer/check/report/situation", method = RequestMethod.GET)
    public List<CheckerReportSituation>  getCheckerReportSituation() {
        return new ArrayList<>(keeperContainerUsedInfoMsgCollector.getDcCheckerReportSituationMap().values());
    }

    @RequestMapping(value = "/keepercontainer/diskType", method = RequestMethod.GET)
    public Set<String> getAllDiskTypeName() {
        try {
            Set<String> diskTypes = new HashSet<>();
            List<ConfigModel> configs = configService.getConfigs(KEY_KEEPER_CONTAINER_STANDARD);
            for (ConfigModel configModel : configs) {
                diskTypes.add(configModel.getSubKey().split(KeeperContainerDiskType.DEFAULT.interval)[0]);
            }
            return diskTypes;
        } catch (Exception e) {
            logger.error("[getAllDiskTypeName]", e);
            return Collections.emptySet();
        }
    }

    @RequestMapping(value = "/keepercontainer/keepers/{ip}")
    public List<KeeperMsgModel> getLocateKeeperContainerByIpAndPort(@PathVariable String ip) {
        try {
            return keeperContainerService.getAllKeepers(ip);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

}
