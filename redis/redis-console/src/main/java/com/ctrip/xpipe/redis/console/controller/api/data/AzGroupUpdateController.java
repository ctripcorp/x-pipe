package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzGroupCreateInfo;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.service.AzGroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class AzGroupUpdateController extends AbstractConsoleController {

    @Autowired
    private AzGroupService azGroupService;

    @RequestMapping(value = "/azGroup/all", method = RequestMethod.GET)
    public List<AzGroupModel> getAllAzGroups() {
        try {
            return azGroupService.getAll();
        } catch (Throwable th) {
            logger.error("[getAllAzGroups][fail]", th);
            return Collections.emptyList();
        }
    }

    @RequestMapping(value = "/azGroup", method = RequestMethod.POST)
    public RetMessage createAzGroup(@RequestBody AzGroupCreateInfo createInfo) {
        try {
            createInfo.check();
            azGroupService.create(createInfo.getName(), createInfo.getAzs());
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[createAzGroup][fail] {}", createInfo, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/azGroup/{name:.+}", method = RequestMethod.DELETE)
    public RetMessage deleteAzGroupByName(@PathVariable String name) {
        try {
            logger.info("[deleteAzGroupByName] {}", name);
            azGroupService.deleteByName(name);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[deleteAzGroupByName][fail] {}", name, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

}
