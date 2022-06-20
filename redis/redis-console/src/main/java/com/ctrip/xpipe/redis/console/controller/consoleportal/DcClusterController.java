package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.DcClusterModel;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class DcClusterController extends AbstractConsoleController {

    @Autowired
    DcClusterService dcClusterService;

    @RequestMapping("/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}")
    public DcClusterModel findDcClusterModel(@PathVariable String clusterName, @PathVariable String dcName) {
        return dcClusterService.findDcClusterModel(clusterName, dcName);
    }
}
