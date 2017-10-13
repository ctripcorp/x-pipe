package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.api.migration.DC_TRANSFORM_DIRECTION;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ShardCreateInfo;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.ClusterShardCounter;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class MetaUpdate extends AbstractConsoleController {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private SentinelService sentinelService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private OrganizationService organizationService;

    @RequestMapping(value = "/stats", method = RequestMethod.GET)
    public Map<String, Integer> getStats() {

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        ClusterShardCounter counter = new ClusterShardCounter();
        xpipeMeta.accept(counter);

        HashMap<String, Integer> counts = new HashMap<>();
        counts.put("clusterCount", counter.getClusterCount());
        counts.put("shardCount", counter.getShardCount());
        return counts;
    }


    @RequestMapping(value = "/clusters", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage createCluster(@RequestBody ClusterCreateInfo outerClusterCreateInfo) {

        ClusterCreateInfo clusterCreateInfo = transform(outerClusterCreateInfo, DC_TRANSFORM_DIRECTION.OUTER_TO_INNER);

        logger.info("[createCluster]{}", clusterCreateInfo);
        List<DcTbl> dcs = new LinkedList<>();
        try {
            clusterCreateInfo.check();

            ClusterTbl clusterTbl = clusterService.find(clusterCreateInfo.getClusterName());
            if (clusterTbl != null) {
                return RetMessage.createFailMessage(String.format("cluster:%s already exist", clusterCreateInfo.getClusterName()));
            }
            for (String dcName : clusterCreateInfo.getDcs()) {
                DcTbl dcTbl = dcService.find(dcName);
                if (dcTbl == null) {
                    return RetMessage.createFailMessage("dc not exist:" + dcName);
                }
                dcs.add(dcTbl);
            }
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        ClusterModel clusterModel = new ClusterModel();
        OrganizationTbl organizationTbl;
        try {
            organizationTbl = getOrganizationTbl(clusterCreateInfo);
            clusterCreateInfo.check();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(dcs.get(0).getId())
                .setClusterName(clusterCreateInfo.getClusterName())
                .setClusterDescription(clusterCreateInfo.getDesc())
                .setClusterAdminEmails(clusterCreateInfo.getClusterAdminEmails())
                .setOrganizationInfo(organizationTbl)
        );


        clusterModel.setSlaveDcs(dcs.subList(1, dcs.size()));
        clusterService.createCluster(clusterModel);
        return RetMessage.createSuccessMessage();
    }

    private OrganizationTbl getOrganizationTbl(ClusterCreateInfo clusterCreateInfo) {
        Long organizationId = clusterCreateInfo.getOrganizationId();
        if(organizationId == null) {
            throw new IllegalStateException("organizationId is required");
        }
        OrganizationTbl organizationTbl = organizationService
            .getOrganizationTblByCMSOrganiztionId(organizationId);
        // If not exists, pull from cms first
        if(organizationTbl == null) {
            organizationService.updateOrganizations();
            organizationTbl = organizationService
                .getOrganizationTblByCMSOrganiztionId(organizationId);
            if(organizationTbl == null) {
                throw new IllegalStateException("Organization Id: " + organizationId + ", could not be found");
            }
        }
        return organizationTbl;

    }

    private ClusterCreateInfo transform(ClusterCreateInfo clusterCreateInfo, DC_TRANSFORM_DIRECTION direction) {

        List<String> dcs = clusterCreateInfo.getDcs();
        List<String> trans = new LinkedList<>();

        for (String dc : dcs) {

            String transfer = direction.transform(dc);
            if (!Objects.equals(transfer, dc)) {
                logger.info("[transform]{}->{}", dc, transfer);
            }
            trans.add(transfer);
        }
        clusterCreateInfo.setDcs(trans);
        return clusterCreateInfo;
    }

    @RequestMapping(value = "/cluster", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage updateCluster(@RequestBody ClusterCreateInfo clusterInfo) {
        return updateSingleCluster(clusterInfo);
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage updateClusters(@RequestBody List<ClusterCreateInfo> clusterInfos) {
        for(ClusterCreateInfo clusterCreateInfo : clusterInfos) {
            RetMessage retMessage = updateSingleCluster(clusterCreateInfo);
            if(ObjectUtils.equals(retMessage.getState(), RetMessage.FAIL_STATE))
                return retMessage;
        }
        return RetMessage.createSuccessMessage();
    }

    private Long getOrganizationId(ClusterCreateInfo clusterCreateInfo) {
        OrganizationTbl organizationTbl = getOrganizationTbl(clusterCreateInfo);
        Long id = organizationTbl.getId();
        return id == null ? 0L : id;
    }

    private RetMessage updateSingleCluster(ClusterCreateInfo clusterInfo) {
        boolean needUpdate = false;
        try {
            ClusterTbl clusterTbl = clusterService.find(clusterInfo.getClusterName());
            if(clusterTbl == null) {
                String message = String.format("cluster not found: %s", clusterInfo.getClusterName());
                return RetMessage.createFailMessage(message);
            }
            Long clusterOrgId = getOrganizationId(clusterInfo);
            if(!ObjectUtils.equals(clusterTbl.getClusterOrgId(), clusterOrgId)) {
                needUpdate = true;
                clusterTbl.setClusterOrgId(clusterOrgId);
            }
            if(!ObjectUtils.equals(clusterTbl.getClusterAdminEmails(), clusterInfo.getClusterAdminEmails())) {
                needUpdate = true;
                clusterTbl.setClusterAdminEmails(clusterInfo.getClusterAdminEmails());
            }
            if(needUpdate) {
                clusterService.update(clusterTbl);
            } else {
                String message = String.format("No field changes for cluster: %s", clusterInfo.getClusterName());
                return RetMessage.createSuccessMessage(message);
            }
        } catch (Exception e) {
            logger.error("{}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.GET)
    public List<ClusterCreateInfo> getClusters() {

        logger.info("[getClusters]");

        List<ClusterTbl> allClusters = clusterService.findAllClustersWithOrgInfo();

        List<ClusterCreateInfo> result = new LinkedList<>();
        allClusters.forEach(clusterTbl -> {

            ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
            clusterCreateInfo.setDesc(clusterTbl.getClusterDescription());
            clusterCreateInfo.setClusterName(clusterTbl.getClusterName());
            OrganizationTbl organizationTbl = clusterTbl.getOrganizationInfo();
            clusterCreateInfo.setOrganizationId(organizationTbl != null ? organizationTbl.getOrgId() : 0L);
            clusterCreateInfo.setClusterAdminEmails(clusterTbl.getClusterAdminEmails());

            List<DcTbl> clusterRelatedDc = dcService.findClusterRelatedDc(clusterTbl.getClusterName());
            clusterRelatedDc.forEach(dcTbl -> {

                if (dcTbl.getId() == clusterTbl.getActivedcId()) {
                    clusterCreateInfo.addFirstDc(dcTbl.getDcName());
                } else {
                    clusterCreateInfo.addDc(dcTbl.getDcName());
                }
            });
            result.add(clusterCreateInfo);
        });

        return transformFromInner(result);
    }

    private List<ClusterCreateInfo> transformFromInner(List<ClusterCreateInfo> source) {

        List<ClusterCreateInfo> results = new LinkedList<>();
        source.forEach(clusterCreateInfo -> results.add(transform(clusterCreateInfo, DC_TRANSFORM_DIRECTION.INNER_TO_OUTER)));
        return results;
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage createShards(@PathVariable String clusterName, @RequestBody List<ShardCreateInfo> shards) {

        logger.info("[createShards]{}, {}", clusterName, shards);

        ClusterTbl clusterTbl = null;

        try {
            clusterTbl = clusterService.find(clusterName);
            if (clusterTbl == null) {
                return RetMessage.createFailMessage("cluster not exist");
            }
            for (ShardCreateInfo shardCreateInfo : shards) {
                shardCreateInfo.check();
            }
        } catch (CheckFailException e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        Map<Long, SetinelTbl> randomSentinelByDc = sentinelService.eachRandomSentinelByDc();
        List<String> successShards = new LinkedList<>();
        List<String> failShards = new LinkedList<>();

        for (ShardCreateInfo shardCreateInfo : shards) {

            try {
                ShardTbl shardTbl = new ShardTbl()
                        .setSetinelMonitorName(shardCreateInfo.getShardMonitorName())
                        .setShardName(shardCreateInfo.getShardName());
                shardService.createShard(clusterName, shardTbl, randomSentinelByDc);
                successShards.add(shardCreateInfo.getShardName());
            } catch (Exception e) {
                logger.error("[createShards]" + clusterName + "," + shardCreateInfo.getShardName(), e);
                failShards.add(shardCreateInfo.getShardName());
            }
        }

        if (failShards.size() == 0) {
            return RetMessage.createSuccessMessage();
        } else {
            StringBuilder sb = new StringBuilder();
            if (successShards.size() > 0) {
                sb.append(String.format("success shards:%s", joiner.join(successShards)));
            }
            sb.append(String.format("fail shards:%s", joiner.join(failShards)));
            return RetMessage.createFailMessage(sb.toString());
        }
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public List<ShardCreateInfo> getShards(@PathVariable String clusterName) {

        List<ShardTbl> allByClusterName = shardService.findAllByClusterName(clusterName);
        List<ShardCreateInfo> result = new LinkedList<>();

        allByClusterName.forEach(shardTbl -> result.add(new ShardCreateInfo(shardTbl.getShardName(), shardTbl.getSetinelMonitorName())));
        return result;
    }

}
