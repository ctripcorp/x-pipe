package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class LogicalBuController extends AbstractConsoleController {

    @Autowired
    private LogicalBuService logicalBuService;

    @RequestMapping(value = "/logical-bus", method = RequestMethod.GET)
    public List<LogicalBuModel> findAll() {
        return logicalBuService.findAll();
    }

    @RequestMapping(value = "/logical-bus/{id}", method = RequestMethod.GET)
    public LogicalBuModel findById(@PathVariable long id) {
        return logicalBuService.findById(id);
    }

    @RequestMapping(value = "/logical-bus", method = RequestMethod.POST)
    public LogicalBuModel create(@RequestBody LogicalBuModel model) {
        return logicalBuService.create(model);
    }

    @RequestMapping(value = "/logical-bus/{id}", method = RequestMethod.PUT)
    public LogicalBuModel update(@PathVariable long id, @RequestBody LogicalBuModel model) {
        return logicalBuService.update(id, model);
    }

    @RequestMapping(value = "/logical-bus/{id}", method = RequestMethod.DELETE)
    public void delete(@PathVariable long id) {
        logicalBuService.delete(id);
    }
}
