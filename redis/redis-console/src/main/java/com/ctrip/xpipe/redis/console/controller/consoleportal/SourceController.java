package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.SourceModel;
import com.ctrip.xpipe.redis.console.service.model.SourceModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class SourceController extends AbstractConsoleController {

    @Autowired
    private SourceModelService sourceModelService;

    @RequestMapping("/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}/sources")
    public List<SourceModel> getSourceModels(@PathVariable String clusterName, @PathVariable String dcName) {
        return sourceModelService.getAllSourceModels(dcName, clusterName);
    }

}
