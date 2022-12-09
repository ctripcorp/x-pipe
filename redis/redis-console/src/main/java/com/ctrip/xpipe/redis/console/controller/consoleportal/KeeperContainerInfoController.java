package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
}
