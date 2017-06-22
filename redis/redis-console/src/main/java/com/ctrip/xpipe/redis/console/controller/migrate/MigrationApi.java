package com.ctrip.xpipe.redis.console.controller.migrate;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.console.controller.migrate.meta.*;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 21, 2017
 */
@RestController
@RequestMapping("/api/migration")
public class MigrationApi extends AbstractConsoleController {

    private int ticketId = 1;

    @RequestMapping(value = "/checkandprepare", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public CheckPrepareResponse checkAndPrepare(@RequestBody(required = true) CheckPrepareRequest checkMeta) {

        logger.info("[checkAndPrepare]{}", checkMeta);

        CheckPrepareResponse checkRetMeta = new CheckPrepareResponse();

        checkRetMeta.setTicketId(ticketId);

        List<CheckPrepareClusterResponse> results = new LinkedList<>();
        results.add(CheckPrepareClusterResponse.createSuccessResponse("cluster1"));
        results.add(CheckPrepareClusterResponse.createFailResponse("cluster2",
                CHECK_FAIL_STATUS.ALREADY_MIGRATING, "0"));
        checkRetMeta.addCheckPrepareClusterResponse(results);
        return checkRetMeta;
    }

    @RequestMapping(value = "/domigration", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public DoResponse doMigrate(@RequestBody(required = true) DoRequest request) {

        logger.info("[doMigrate]{}", request);
        return new DoResponse(true, "success!");
    }

    @RequestMapping(value = "/checkstatus/{ticketId}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public CheckStatusResponse checkStatus(@PathVariable int ticketId) {

        logger.info("[checkStatus]{}", ticketId);

        CheckStatusResponse response = new CheckStatusResponse();

        response.addResult(
                new CheckStatusClusterResponse("cluster1", DO_STATUS.INITED, 0, "inited")
        );
        response.addResult(
                new CheckStatusClusterResponse("cluster2", DO_STATUS.SUCCESS, 0, "")
        );
        return response;
    }

    @RequestMapping(value = "/rollback", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public RollbackResponse rollback(@RequestBody(required = true) RollbackRequest request) {

        logger.info("[rollback]{}", request);

        RollbackResponse response = new RollbackResponse();
        return new RollbackResponse(true, "success");
    }

}
