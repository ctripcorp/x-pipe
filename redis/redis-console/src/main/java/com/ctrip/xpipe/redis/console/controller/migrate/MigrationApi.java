package com.ctrip.xpipe.redis.console.controller.migrate;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.console.controller.migrate.meta.*;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterActiveDcNotRequest;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigratingNow;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.console.service.migration.impl.TryMigrateResult;
import com.google.common.base.Joiner;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private MigrationService migrationService;

    private int ticketId = 1;

    @RequestMapping(value = "/checkandprepare", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public CheckPrepareResponse checkAndPrepare(@RequestBody(required = true) CheckPrepareRequest checkMeta) {

        logger.info("[checkAndPrepare]{}", checkMeta);

        CheckPrepareResponse checkRetMeta = new CheckPrepareResponse();

        List<TryMigrateResult> maySuccessClusters = new LinkedList<>();
        List<CheckPrepareClusterResponse> failClusters = new LinkedList<>();

        String fromIdc = checkMeta.getFromIdc();
        for(String clusterName : checkMeta.getClusters()){
            try {
                TryMigrateResult tryMigrateResult = migrationService.tryMigrate(clusterName, fromIdc);
                maySuccessClusters.add(tryMigrateResult);
                logger.info("[checkAndPrepare]{}", tryMigrateResult);
            } catch (ClusterNotFoundException e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.CLUSTER_NOT_FOUND, e.getMessage()));
                logger.error("[checkAndPrepare]" + clusterName, e);
            } catch (ClusterActiveDcNotRequest e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.ACTIVE_DC_ALREADY_NOT_REQUESTED, e.getMessage()));
                logger.error("[checkAndPrepare]" + clusterName, e);
            } catch (ClusterMigratingNow e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.ALREADY_MIGRATING, String.valueOf(e.getEventId())));
                logger.error("[checkAndPrepare]" + clusterName, e);
            }catch (Exception e){
                logger.error("[checkAndPrepare]" + clusterName, e);
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.OTHERS, e.getMessage()));
            }
        }

        Long eventId = -1L;
        if(maySuccessClusters.size() > 0){
            MigrationRequest request = new MigrationRequest("api_call");
            request.setTag("api_call");
            maySuccessClusters.forEach((tryMigrateResult) -> request.addClusterInfo(new MigrationRequest.ClusterInfo(tryMigrateResult)));
            eventId = migrationService.createMigrationEvent(request);
        }

        checkRetMeta.setTicketId(eventId);
        failClusters.forEach((failCluster) -> checkRetMeta.addCheckPrepareClusterResponse(failCluster));
        maySuccessClusters.forEach((successCluster) -> checkRetMeta.addCheckPrepareClusterResponse(CheckPrepareClusterResponse.createSuccessResponse(successCluster.getClusterName(), successCluster.getFromDcName(), successCluster.getToDcName())));
        return checkRetMeta;
    }

    @RequestMapping(value = "/domigration", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public DoResponse doMigrate(@RequestBody(required = true) DoRequest request) {

        logger.info("[doMigrate]{}", request);

        try{
            migrationService.continueMigrationEvent(request.getTicketId());
            return new DoResponse(true, "success!");
        }catch (Exception e){
            logger.error("[doMigrate]" + request, e);
            return new DoResponse(false, e.getMessage());
        }
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

        long tickedId = request.getTicketId();

        if(request.getClusters() == null || request.getClusters().size() == 0){
            return new RollbackResponse();
        }

        RollbackResponse rollbackResponse = new RollbackResponse();

        List<String> success = new LinkedList<>();
        List<String> errors = new LinkedList<>();

        for(String clusterName : request.getClusters()){

            MigrationCluster migrationCluster = null;
            try {
                migrationCluster = migrationService.rollbackMigrationCluster(tickedId, clusterName);
                rollbackResponse.addResult(new RollbackClusterResponse(true, clusterName, migrationCluster.fromDc(), migrationCluster.destDc(), "success"));
            } catch (ClusterNotFoundException e) {
                logger.error("[rollback]" + clusterName, e);
                rollbackResponse.addResult(new RollbackClusterResponse(false, clusterName, e.getMessage()));
            }catch (Exception e){
                logger.error("[rollback]" + clusterName, e);
                if(migrationCluster == null){
                    rollbackResponse.addResult(new RollbackClusterResponse(false, clusterName, e.getMessage()));
                }else{
                    rollbackResponse.addResult(new RollbackClusterResponse(false, clusterName, migrationCluster.fromDc(), migrationCluster.destDc(), e.getMessage()));
                }
            }
        }
        return rollbackResponse;
    }
}
