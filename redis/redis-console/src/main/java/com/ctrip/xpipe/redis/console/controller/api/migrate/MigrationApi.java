package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.*;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.console.service.migration.impl.TryMigrateResult;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 21, 2017
 */
@RestController
@RequestMapping("/api/migration")
public class MigrationApi extends AbstractConsoleController {

    private DcMapper dcMapper = DcMapper.INSTANCE;

    @Autowired
    private MigrationService migrationService;

    @RequestMapping(value = "/checkandprepare", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public CheckPrepareResponse checkAndPrepare(@RequestBody(required = true) CheckPrepareRequest checkMeta) {

        mapRequestIdc(checkMeta);

        logger.info("[checkAndPrepare]{}", checkMeta);
        CheckPrepareResponse checkRetMeta = new CheckPrepareResponse();

        List<TryMigrateResult> availableClusters = new LinkedList<>();
        List<ClusterMigratingNow> migratingClusters = new LinkedList<>();
        List<CheckPrepareClusterResponse> failClusters = new LinkedList<>();

        String fromIdc = checkMeta.getFromIdc();
        for (String clusterName : checkMeta.getClusters()) {
            try {
                TryMigrateResult tryMigrateResult = migrationService.tryMigrate(clusterName, fromIdc, checkMeta.getToIdc());
                availableClusters.add(tryMigrateResult);
                logger.info("[checkAndPrepare]{}", tryMigrateResult);
            } catch (ClusterNotFoundException e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.CLUSTER_NOT_FOUND, e.getMessage()));
                logger.error("[checkAndPrepare]" + clusterName, e);
            } catch (MigrationNotSupportException e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.CLUSTER_NOT_SUPPORT_MIGRATION, e.getMessage()));
                logger.error("[checkAndPrepare]" + clusterName, e);
            } catch (ClusterActiveDcNotRequest e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.ACTIVE_DC_ALREADY_NOT_REQUESTED, e.getMessage()));
                logger.error("[checkAndPrepare]" + clusterName, e);
            } catch (ClusterMigratingNow e) {
                logger.warn("[checkAndPrepare]" + clusterName, e);
                migratingClusters.add(e);
            } catch (ClusterMigratingNowButMisMatch e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, e.getFromIdc(), e.getToIdc(), CHECK_FAIL_STATUS.ALREADY_MIGRATING, String.valueOf(e.getEventId())));
                logger.warn("[checkAndPrepare]" + clusterName, e);
            } catch (MigrationSystemNotHealthyException e) {
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.MIGRATION_SYSTEM_UNHEALTHY, String.valueOf(e.getMessage())));
                logger.error("[checkAndPrepare]" + clusterName, e);
            } catch (Exception e) {
                logger.error("[checkAndPrepare]" + clusterName, e);
                failClusters.add(CheckPrepareClusterResponse.createFailResponse(clusterName, fromIdc, CHECK_FAIL_STATUS.OTHERS, e.getMessage()));
            }
        }

        Long eventId = -1L;
        if (availableClusters.size() > 0) {
            MigrationRequest request = new MigrationRequest("api_call");
            request.setTag("api_call");
            logger.info("[checkAndPrepare][availableClusters]{}", availableClusters);
            availableClusters.forEach((tryMigrateResult) -> request.addClusterInfo(new MigrationRequest.ClusterInfo(tryMigrateResult)));
            eventId = migrationService.createMigrationEvent(request);
        }else if(migratingClusters.size() > 0){
            int migraingSelected = RandomUtils.nextInt(0, migratingClusters.size());
            ClusterMigratingNow clusterMigratingNow = migratingClusters.remove(migraingSelected);
            eventId = clusterMigratingNow.getEventId();
            availableClusters.add(new TryMigrateResult(clusterMigratingNow.getClusterName(), clusterMigratingNow.getFromIdc(), clusterMigratingNow.getToIdc()));
            logger.info("[checkAndPrepare][clusterAlreadyMigrating, random choose one to return]{}", clusterMigratingNow.getMessage());
        }

        checkRetMeta.setTicketId(eventId);

        availableClusters.forEach((successCluster) -> checkRetMeta.addCheckPrepareClusterResponse(CheckPrepareClusterResponse.createSuccessResponse(successCluster.getClusterName(), successCluster.getFromDcName(), successCluster.getToDcName())));
        migratingClusters.forEach((migratingNow) -> checkRetMeta.addCheckPrepareClusterResponse(CheckPrepareClusterResponse.createFailResponse(
                migratingNow.getClusterName(), migratingNow.getFromIdc(), migratingNow.getToIdc(), CHECK_FAIL_STATUS.ALREADY_MIGRATING, String.valueOf(migratingNow.getEventId()))));
        failClusters.forEach((failCluster) -> checkRetMeta.addCheckPrepareClusterResponse(failCluster));
        mapResponseIdc(checkRetMeta.getResults());
        return checkRetMeta;
    }

    @RequestMapping(value = "/domigration", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public DoResponse doMigrate(@RequestBody(required = true) DoRequest request) {

        logger.info("[doMigrate]{}", request);

        try {
            migrationService.continueMigrationEvent(request.getTicketId());
            return new DoResponse(true, "success!");
        } catch (Exception e) {
            logger.error("[doMigrate]" + request, e);
            return new DoResponse(false, e.getMessage());
        }
    }

    @RequestMapping(value = "/checkstatus/{ticketId}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public CheckStatusResponse checkStatus(@PathVariable int ticketId) {

        logger.info("[checkStatus]{}", ticketId);
        CheckStatusResponse response = new CheckStatusResponse();

        MigrationEvent migrationEvent = migrationService.getMigrationEvent(ticketId);
        if (migrationEvent == null) {
            logger.error("[checkStatus][can not find eventId]{}", ticketId);
            return response;
        }

        migrationEvent.getMigrationClusters().forEach(migrationCluster -> {
                    String clusterName = migrationCluster.clusterName();
                    MigrationStatus migrationStatus = migrationCluster.getStatus();
                    CheckStatusClusterResponse checkResponse = new CheckStatusClusterResponse(clusterName,
                            DO_STATUS.fromMigrationStatus(migrationStatus),
                            migrationStatus.getPercent(),
                            migrationStatus.toString());
                    checkResponse.setFromIdc(migrationCluster.fromDc());
                    checkResponse.setToIdc(migrationCluster.destDc());
                    response.addResult(checkResponse);
                }
        );

        mapResponseIdc(response.getResults());
        return response;
    }

    @RequestMapping(value = "/rollback", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    public RollbackResponse rollback(@RequestBody(required = true) RollbackRequest request) {

        logger.info("[rollback]{}", request);

        long tickedId = request.getTicketId();

        if (request.getClusters() == null || request.getClusters().size() == 0) {
            return new RollbackResponse();
        }

        RollbackResponse rollbackResponse = new RollbackResponse();

        for (String clusterName : request.getClusters()) {

            MigrationCluster migrationCluster = null;
            try {
                migrationCluster = migrationService.rollbackMigrationCluster(tickedId, clusterName);
                rollbackResponse.addResult(new RollbackClusterResponse(true, clusterName, migrationCluster.fromDc(), migrationCluster.destDc(), "success"));
            } catch (ClusterNotFoundException e) {
                logger.error("[rollback]" + clusterName, e);
                rollbackResponse.addResult(new RollbackClusterResponse(false, clusterName, e.getMessage()));
            } catch (Exception e) {
                logger.error("[rollback]" + clusterName, e);
                if (migrationCluster == null) {
                    rollbackResponse.addResult(new RollbackClusterResponse(false, clusterName, e.getMessage()));
                } else {
                    rollbackResponse.addResult(new RollbackClusterResponse(false, clusterName, migrationCluster.fromDc(), migrationCluster.destDc(), e.getMessage()));
                }
            }
        }

        mapResponseIdc(rollbackResponse.getResults());
        return rollbackResponse;
    }

    @RequestMapping(value = "/migration/system/health/status", method = RequestMethod.GET)
    public RetMessage getMigrationSystemHealthStatus() {
        logger.info("[getMigrationSystemHealthStatus]");
        try {
            return migrationService.getMigrationSystemHealth();
        } catch (Exception e) {
            logger.error("[getMigrationSystemHealthStatus]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }


    private void mapResponseIdc(List<? extends AbstractClusterMeta> results) {

        results.forEach(clusterMeta -> {

            String fromIdc = clusterMeta.getFromIdc();
            String toIdc = clusterMeta.getToIdc();

            String mappingFrom = dcMapper.getDc(fromIdc);
            String mappingTo = dcMapper.getDc(toIdc);

            if(mappingFrom != null){
                clusterMeta.setFromIdc(mappingFrom);
                logger.debug("[mapResponseIdc]{} -> {}", fromIdc, mappingFrom);
            }

            if(mappingTo != null){
                clusterMeta.setToIdc(mappingTo);
                logger.debug("[mapResponseIdc]{} -> {}", toIdc, mappingTo);
            }

        });
    }

    private void mapRequestIdc(CheckPrepareRequest checkMeta) {

        String fromIdcReverse = dcMapper.reverse(checkMeta.getFromIdc());
        if(fromIdcReverse != null){
            logger.debug("[checkRequestIdc][reverse dc]{} -> {}", checkMeta.getFromIdc(), fromIdcReverse);
            checkMeta.setFromIdc(fromIdcReverse);
        }

        String toIdcReverse = dcMapper.reverse(checkMeta.getToIdc());
        if(toIdcReverse != null){
            logger.debug("[checkRequestIdc][reverse dc]{} -> {}", checkMeta.getToIdc(), toIdcReverse);
            checkMeta.setToIdc(toIdcReverse);
        }

    }

    @VisibleForTesting
    protected void setMigrationService(MigrationService migrationService) {
        this.migrationService = migrationService;
    }
}
