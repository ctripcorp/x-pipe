package com.ctrip.xpipe.redis.console.controller.api;


import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class KeeperContainerOverloadController extends AbstractConsoleController{

    @Autowired
    KeeperContainerUsedInfoAnalyzer analyzer;

    @RequestMapping(value = "/keepercontainer/overload/info/all", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getAllReadyToMigrateKeeperContainers() {
        return analyzer.getAllDcReadyToMigrationKeeperContainers();
    }
}
