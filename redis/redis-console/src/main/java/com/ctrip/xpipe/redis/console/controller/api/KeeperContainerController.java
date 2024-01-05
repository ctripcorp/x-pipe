package com.ctrip.xpipe.redis.console.controller.api;


import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.entity.KeeperContainerDiskType;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.apache.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_CONTAINER_STANDARD;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class KeeperContainerController extends AbstractConsoleController{

    @Autowired
    KeeperContainerUsedInfoAnalyzer analyzer;

    @Autowired
    ConfigService configService;

    @RequestMapping(value = "/keepercontainer/overload/info/all", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getAllReadyToMigrateKeeperContainers() {
        return analyzer.getCurrentDcReadyToMigrationKeeperContainers();
    }

    @RequestMapping(value = "/keepercontainer/info/all", method = RequestMethod.GET)
    public List<KeeperContainerUsedInfoModel> getAllKeeperContainerUsedInfoModelsList() {
        return analyzer.getCurrentDcKeeperContainerUsedInfoModelsList();
    }

    @RequestMapping(value = "/keepercontainer/full/synchronization/time", method = RequestMethod.GET)
    public List<Integer> getMaxKeeperContainerFullSynchronizationTime() {
        return analyzer.getCurrentDcMaxKeeperContainerFullSynchronizationTime();
    }

    @RequestMapping(value = "/keepercontainer/diskType", method = RequestMethod.POST)
    public RetMessage setDiskType(@RequestBody ConfigModel configModel) {
        try {
            configModel.setKey(KEY_KEEPER_CONTAINER_STANDARD);
            configService.setKeyKeeperContainerStandard(configModel);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepercontainer/diskType", method = RequestMethod.GET)
    public RetMessage getDiskType() {
        try {
            Map<String, String> map = new HashMap<>();
            for (KeeperContainerDiskType value : KeeperContainerDiskType.values()) {
                map.put(value.getPeerData(), configService.getConfig(KEY_KEEPER_CONTAINER_STANDARD, value.getPeerData()).getVal());
                map.put(value.getInputFlow(), configService.getConfig(KEY_KEEPER_CONTAINER_STANDARD, value.getInputFlow()).getVal());
            }
            return RetMessage.createSuccessMessage(map.toString());
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }


}
