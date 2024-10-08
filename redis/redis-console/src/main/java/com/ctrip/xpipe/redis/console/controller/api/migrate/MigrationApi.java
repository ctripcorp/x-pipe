package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.*;
import com.ctrip.xpipe.redis.console.entity.ClusterEntity;
import com.ctrip.xpipe.redis.console.entity.MigrationBiClusterEntity;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.repository.ClusterRepository;
import com.ctrip.xpipe.redis.console.repository.MigrationBiClusterRepository;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.console.service.migration.impl.TryMigrateResult;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    @Autowired
    private MigrationBiClusterRepository biMigrationRepository;

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private DcCache dcCache;

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

    @GetMapping(value = "history")
    public Map<String, List<ClusterMigrationStatus>> getClusterMigrationHistory(@RequestBody MigrationHistoryReq req) {
        logger.info("[history][{}-{}] {}", req.from, req.to, req.clusters);
        long current = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        if (req.from < 0 || req.from >= current) return Collections.emptyMap();
        if (null == req.clusters || req.clusters.isEmpty()) return Collections.emptyMap();
        if (req.to < req.from) req.to = current;

        List<MigrationClusterTbl> migrationClusterTbls = migrationService.fetchMigrationClusters(req.clusters,
                TimeUnit.SECONDS.toMillis(req.from), TimeUnit.SECONDS.toMillis(req.to));
        Map<String, List<ClusterMigrationStatus>> resp = new HashMap<>();
        migrationClusterTbls.forEach(migrationClusterTbl -> {
            String clusterName = migrationClusterTbl.getCluster().getClusterName();
            if (!resp.containsKey(clusterName)) resp.put(clusterName, new ArrayList<>());
            ClusterMigrationStatus clusterMigrationStatus = ClusterMigrationStatus.from(migrationClusterTbl, dcCache);
            resp.get(clusterName).add(clusterMigrationStatus);
        });

        return resp;
    }

    @GetMapping(value = "/v2/history")
    public Map<String, List<ClusterMigrationStatusV2>> getClusterMigrationHistoryV2(@RequestBody MigrationHistoryReq req) {
        logger.info("[historyV2][{}-{}] {}", req.from, req.to, req.clusters);
        long current = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        if (req.from < 0 || req.from >= current) return Collections.emptyMap();
        if (null == req.clusters || req.clusters.isEmpty()) return Collections.emptyMap();
        if (req.to < req.from) req.to = current;

        List<ClusterEntity> clusters = clusterRepository.selectAllByClusterName(req.clusters);
        Map<String, ClusterEntity> oneWayClusters = new HashMap<>();
        Map<Long, ClusterEntity> biDirectionClusters = new HashMap<>();

        for (ClusterEntity cluster: clusters) {
            if (ClusterType.isSameClusterType(cluster.getClusterType(), ClusterType.ONE_WAY)) {
                oneWayClusters.put(cluster.getClusterName(), cluster);
            } else if (ClusterType.isSameClusterType(cluster.getClusterType(), ClusterType.BI_DIRECTION)) {
                biDirectionClusters.put(cluster.getId(), cluster);
            }
        }

        long from = TimeUnit.SECONDS.toMillis(req.from);
        long to = TimeUnit.SECONDS.toMillis(req.to);
        Map<String, List<ClusterMigrationStatusV2>> resp = new HashMap<>();

        if (!oneWayClusters.isEmpty()) {
            List<MigrationClusterTbl> migrationClusterTbls = migrationService.fetchMigrationClusters(oneWayClusters.keySet(), from, to);
            for (MigrationClusterTbl migrationClusterTbl: migrationClusterTbls) {
                String clusterName = migrationClusterTbl.getCluster().getClusterName();
                ClusterEntity cluster = oneWayClusters.get(clusterName);
                if (!resp.containsKey(clusterName)) resp.put(clusterName, new ArrayList<>());
                ClusterMigrationStatusV2 clusterMigrationStatus = ClusterMigrationStatusV2.from(cluster, migrationClusterTbl, dcCache);
                resp.get(clusterName).add(clusterMigrationStatus);
            }
        }

        if (!biDirectionClusters.isEmpty()) {
            List<MigrationBiClusterEntity> biMigrationRecords = biMigrationRepository.selectAllByClusterIdAndOpTime(biDirectionClusters.keySet(),
                    new Date(from), new Date(to));
            for (MigrationBiClusterEntity biMigrationRecord: biMigrationRecords) {
                Long clusterId = biMigrationRecord.getClusterId();
                ClusterEntity cluster = biDirectionClusters.get(clusterId);
                String clusterName = cluster.getClusterName();
                if (!resp.containsKey(clusterName)) resp.put(clusterName, new ArrayList<>());
                ClusterMigrationStatusV2 clusterMigrationStatus = ClusterMigrationStatusV2.from(cluster, biMigrationRecord, getBiRelatedDcs(clusterName));
                resp.get(clusterName).add(clusterMigrationStatus);
            }
        }

        return resp;
    }

    private Set<String> getBiRelatedDcs(String clusterId) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        for (DcMeta dcMeta: xpipeMeta.getDcs().values()) {
            if (dcMeta.getClusters().containsKey(clusterId)) {
                ClusterMeta cluster = dcMeta.getClusters().get(clusterId);
                String dcs = cluster.getDcs();
                if (StringUtil.isEmpty(dcs)) return Collections.emptySet();
                else return new HashSet<>(Arrays.asList(dcs.split("\\s*,\\s*")));
            }
        }
        return Collections.emptySet();
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
