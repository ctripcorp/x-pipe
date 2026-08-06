package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class LogicalBuApiController {

    @Autowired
    private LogicalBuService logicalBuService;

    @RequestMapping(value = "/logical-bus/all", method = RequestMethod.GET)
    public List<LogicalBuModel> findAll() {
        return logicalBuService.findAll();
    }
}
