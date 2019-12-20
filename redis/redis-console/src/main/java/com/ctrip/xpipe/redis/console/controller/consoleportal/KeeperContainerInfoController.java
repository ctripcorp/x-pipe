package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeeperContainerInfoController extends AbstractConsoleController {

    @Autowired
    KeeperContainerService keeperContainerService;

    @RequestMapping(value = "/keepercontainer/infos/all", method = RequestMethod.GET)
    public List<KeeperContainerInfoModel> getAllKeeperContainerInfos() {
        return keeperContainerService.findAllInfos();
    }

}
