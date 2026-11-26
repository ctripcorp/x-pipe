package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.redis.console.service.logicalbu.KeeperOpRequest;
import com.ctrip.xpipe.redis.console.service.logicalbu.KeeperOpResult;
import com.ctrip.xpipe.redis.console.service.logicalbu.KeeperStateResult;
import com.ctrip.xpipe.redis.console.service.logicalbu.LogicalBuKeeperOpsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class LogicalBuController extends AbstractConsoleController {

    @Autowired
    private LogicalBuService logicalBuService;

    @Autowired
    private LogicalBuKeeperOpsService logicalBuKeeperOpsService;

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

    /**
     * Switch the TFS keepers of the given logical BUs to BM: precheck, update priority, notify MetaServer.
     * Synchronous; HTTP 200 with per-BU layered failures, 400 only for a malformed request.
     */
    @RequestMapping(value = "/logical-bus/switchBM", method = RequestMethod.POST)
    public KeeperOpResult switchBM(@RequestBody KeeperOpRequest request) {
        return logicalBuKeeperOpsService.switchBM(request);
    }

    /**
     * Undo switchBM: clear LOGICAL_BU_TBL.priority (NULL) so TFS keepers use their own keeper_priority again,
     * then notify MetaServer. Same response shape as switchBM.
     */
    @RequestMapping(value = "/logical-bus/switchRestore", method = RequestMethod.POST)
    public KeeperOpResult switchRestore(@RequestBody KeeperOpRequest request) {
        return logicalBuKeeperOpsService.switchRestore(request);
    }

    /**
     * Reset TFS keeper health of the given logical BUs (RECOVERED_PENDING → HEALTHY) through their keepercontainers.
     * Priority is not changed. Same response shape as switchBM, plus keeperCount / successKeeperCount per BU.
     */
    @RequestMapping(value = "/logical-bus/resetHealth", method = RequestMethod.POST)
    public KeeperOpResult resetHealth(@RequestBody KeeperOpRequest request) {
        return logicalBuKeeperOpsService.resetHealth(request);
    }

    /**
     * Read-only: TFS keepercontainer counts per BU per IDC by state (HEALTHY / DOWN / PENDING / UNKNOWN),
     * with the containers themselves so UNKNOWN ones can be shown on the page.
     */
    @RequestMapping(value = "/logical-bus/keeperState", method = RequestMethod.POST)
    public KeeperStateResult keeperState(@RequestBody KeeperOpRequest request) {
        return logicalBuKeeperOpsService.keeperState(request);
    }
}
