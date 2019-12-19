package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.KeepercontainerInfoModel;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeeperContainerInfoController extends AbstractConsoleController {

    @Autowired
    KeepercontainerService keepercontainerService;

    @RequestMapping(value = "/keepercontainer/infos/all", method = RequestMethod.GET)
    public List<KeepercontainerInfoModel> getAllKeeperContainerInfos() {
        return keepercontainerService.findAllInfos();
    }

}
