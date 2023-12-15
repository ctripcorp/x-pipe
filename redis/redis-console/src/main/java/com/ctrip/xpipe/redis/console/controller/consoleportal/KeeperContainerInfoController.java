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
//        return analyzer.getAllKeeperContainerUsedInfoModelsList();
        List<KeeperContainerUsedInfoModel> allKeeperContainerUsedInfoModelsList = new ArrayList<>();
        KeeperContainerUsedInfoModel infoModel = new KeeperContainerUsedInfoModel();
        infoModel.setKeeperIp("127.0.1.4");
        infoModel.setDiskType("read10");
        infoModel.setDiskSize(1024*1024*1024*12L);
        infoModel.setDiskUsed(1024*1024*1024*6L);
        infoModel.setDcName("jq");
        infoModel.setActiveInputFlow(123456L);
        infoModel.setTotalInputFlow(234567L);
        infoModel.setActiveRedisUsedMemory(111111L);
        infoModel.setTotalRedisUsedMemory(222222L);
        infoModel.setActiveKeeperCount(5);
        infoModel.setTotalKeeperCount(12);
        infoModel.setDiskAvailable(true);
        infoModel.setRedisUsedMemoryStandard(121111L);
        infoModel.setInputFlowStandard(133456L);
        Map<DcClusterShardActive, KeeperContainerUsedInfoModel.KeeperUsedInfo> map = new HashMap<>();
        DcClusterShardActive dcClusterShardActive1 = new DcClusterShardActive("jq","cluster1","shard11",true,6666);
        KeeperContainerUsedInfoModel.KeeperUsedInfo keeperUsedInfo1 = new KeeperContainerUsedInfoModel.KeeperUsedInfo(11,11,"127.0.1.4");
        DcClusterShardActive dcClusterShardActive2 = new DcClusterShardActive("jq","cluster2","shard21",false,6667);
        KeeperContainerUsedInfoModel.KeeperUsedInfo keeperUsedInfo2 = new KeeperContainerUsedInfoModel.KeeperUsedInfo(22,22,"127.0.1.4");
        map.put(dcClusterShardActive1, keeperUsedInfo1);
        map.put(dcClusterShardActive2, keeperUsedInfo2);
        infoModel.setDetailInfo(map);
        KeeperContainerUsedInfoModel infoModel1 = new KeeperContainerUsedInfoModel();
        infoModel1.setKeeperIp("127.0.1.5");
        infoModel1.setDiskType("read1");
        infoModel1.setDiskSize(1024*1024*1024*6L);
        infoModel1.setDiskUsed(1024*1024*1024*3L);
        infoModel1.setDcName("jq");
        infoModel1.setActiveInputFlow(223456L);
        infoModel1.setTotalInputFlow(334567L);
        infoModel1.setActiveRedisUsedMemory(211111L);
        infoModel1.setTotalRedisUsedMemory(322222L);
        infoModel1.setActiveKeeperCount(11);
        infoModel1.setTotalKeeperCount(12);
        infoModel1.setDiskAvailable(true);
        infoModel1.setRedisUsedMemoryStandard(411111L);
        infoModel1.setInputFlowStandard(423456L);
        //detailInfo
        allKeeperContainerUsedInfoModelsList.add(infoModel);
        allKeeperContainerUsedInfoModelsList.add(infoModel1);
        return allKeeperContainerUsedInfoModelsList;
    }
}
