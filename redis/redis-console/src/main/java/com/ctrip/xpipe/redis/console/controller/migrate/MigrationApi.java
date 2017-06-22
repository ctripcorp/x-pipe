package com.ctrip.xpipe.redis.console.controller.migrate;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.console.controller.migrate.meta.CHECK_FAIL_STATUS;
import com.ctrip.xpipe.redis.console.controller.migrate.meta.CheckPrepareClusterResponse;
import com.ctrip.xpipe.redis.console.controller.migrate.meta.CheckPrepareRequestMeta;
import com.ctrip.xpipe.redis.console.controller.migrate.meta.CheckPrepareResponseMeta;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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
    public CheckPrepareResponseMeta checkAndPrepare(@RequestBody(required = true) CheckPrepareRequestMeta checkMeta) {

        logger.info("[checkAndPrepare]{}", checkMeta);

        CheckPrepareResponseMeta checkRetMeta = new CheckPrepareResponseMeta();

        checkRetMeta.setTicketId(ticketId);

        List<CheckPrepareClusterResponse> results = new LinkedList<>();
        results.add(CheckPrepareClusterResponse.createSuccessResponse("cluster1"));
        results.add(CheckPrepareClusterResponse.createFailResponse("cluster2",
                CHECK_FAIL_STATUS.ALREADY_MIGRATING, "0"));
        checkRetMeta.addCheckPrepareClusterResponse(results);
        return checkRetMeta;
    }

}
