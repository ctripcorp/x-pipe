package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.AppliercontainerInfoModel;
import com.ctrip.xpipe.redis.console.model.AppliercontainerTbl;
import com.ctrip.xpipe.redis.console.service.AppliercontainerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class AppliercontainerInfoController extends AbstractConsoleController {

    @Autowired
    AppliercontainerService appliercontainerService;

    @RequestMapping(value = "/appliercontainer", method = RequestMethod.POST)
    public RetMessage addAppliercontainer(@RequestBody AppliercontainerInfoModel model) {
        logger.info("[addAppliercontainer] add appliercontainer {}", model);
        try {
            appliercontainerService.addAppliercontainerByInfoModel(model);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addAppliercontainer] add appliercontainer {} fail", model, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/appliercontainer", method = RequestMethod.PUT)
    public RetMessage updateAppliercontainer(@RequestBody AppliercontainerInfoModel model) {
        logger.info("[updateAppliercontainer] update appliercontainer {}", model);
        try {
            appliercontainerService.updateAppliercontainerByInfoModel(model);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateAppliercontainer] update appliercontainer {} fail", model, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/appliercontainer/infos/all", method = RequestMethod.GET)
    public List<AppliercontainerInfoModel> getAllAppliercontaienrInfos() {
        return appliercontainerService.findAllAppliercontainerInfoModels();
    }

    @RequestMapping(value = "/appliercontainer/{appliercontainerId}", method = RequestMethod.GET)
    public AppliercontainerInfoModel getAppliercontainerInfoById(@PathVariable long appliercontainerId) {
        return appliercontainerService.findAppliercontainerInfoModelById(appliercontainerId);
    }

    @RequestMapping(value = "/dcs/{dcName}/clusters/{clusterName}/active-appliercontainers", method = RequestMethod.GET)
    public List<AppliercontainerTbl> findBestAppliercontainersByDcCluster(@PathVariable String dcName, @PathVariable String clusterName) {
        List<AppliercontainerTbl> appliercontainers = appliercontainerService.findBestAppliercontainersByDcCluster(dcName, clusterName);
        logger.info("[findBestAppliercontainersByDcCluster] best applier containers {}", appliercontainers);
        return appliercontainers;
    }
}
