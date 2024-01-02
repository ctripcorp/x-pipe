package com.ctrip.xpipe.redis.console.controller.api;


import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class KeeperContainerController extends AbstractConsoleController{

    @Autowired
    KeeperContainerUsedInfoAnalyzer analyzer;

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

    @RequestMapping(value = "/keepercontainer/overload/info/current", method = RequestMethod.GET)
    public List<KeeperContainerUsedInfoModel>  getCurrentReadyToMigrateKeeperContainers() {
        List<KeeperContainerUsedInfoModel> result = new ArrayList<>();
        analyzer.getKeeperContainerUsedInfoModelIndexMap().values().forEach(result::addAll);
        return result;
    }

}
