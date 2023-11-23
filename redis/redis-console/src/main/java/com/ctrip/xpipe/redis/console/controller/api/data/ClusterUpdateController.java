package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.api.migration.DC_TRANSFORM_DIRECTION;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.*;
import com.ctrip.xpipe.redis.console.dto.AzGroupDTO;
import com.ctrip.xpipe.redis.console.dto.ClusterCreateDTO;
import com.ctrip.xpipe.redis.console.dto.ClusterDTO;
import com.ctrip.xpipe.redis.console.dto.ClusterUpdateDTO;
import com.ctrip.xpipe.redis.console.dto.MultiGroupClusterCreateDTO;
import com.ctrip.xpipe.redis.console.dto.SingleGroupClusterCreateDTO;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ClusterUpdateController extends AbstractController {

    @Autowired
    private OrganizationService organizationService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ConsoleConfig config;


    @PostMapping(value = "/clusters")
    public RetMessage createCluster(@RequestBody ClusterCreateInfo outerClusterCreateInfo) {
        ClusterCreateInfo createInfo = transform(outerClusterCreateInfo, DC_TRANSFORM_DIRECTION.OUTER_TO_INNER);
        logger.info("[createCluster]{}", createInfo);
        try {
            createInfo.check();

            OrganizationTbl organizationTbl = this.getOrganizationTbl(createInfo);
            ClusterCreateDTO clusterCreateDTO = new ClusterCreateDTO(createInfo, organizationTbl.getOrgName());

            if (CollectionUtils.isEmpty(createInfo.getRegions())) {
                // 初始版集群，不创建group
                SingleGroupClusterCreateDTO singleGroupClusterCreateDTO =
                    new SingleGroupClusterCreateDTO(clusterCreateDTO, createInfo.getDcs());
                clusterService.createSingleGroupCluster(singleGroupClusterCreateDTO);
            } else {
                // 新版多group集群，可为非对称/海外不同步/双向同步
                String clusterType = createInfo.getClusterType();
                if (!ClusterType.supportMultiGroup(clusterType)) {
                    return RetMessage.createFailMessage(String
                        .format("cluster type - %s does not support multi group", clusterType));
                }

                List<RegionInfo> regions = createInfo.getRegions();
                List<AzGroupDTO> azGroups = regions.stream().map(AzGroupDTO::new).collect(Collectors.toList());

                MultiGroupClusterCreateDTO multiGroupClusterCreateDTO =
                    new MultiGroupClusterCreateDTO(clusterCreateDTO, azGroups);
                clusterService.createMultiGroupCluster(multiGroupClusterCreateDTO);
            }

            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[createCluster]{} Failed, Error: {}", createInfo.getClusterName(), e, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    //synchronously delete cluster, including meta server
    @DeleteMapping(value = "/cluster/" + CLUSTER_NAME_PATH_VARIABLE)
    public RetMessage deleteCluster(@PathVariable String clusterName, @RequestParam(defaultValue = "true") boolean checkEmpty) {
        logger.info("[deleteCluster]{}, checkEmpty-{}", clusterName, checkEmpty);
        try {
            if (checkEmpty && clusterService.containsRedisInstance(clusterName)) {
                logger.info("[deleteCluster]{} check empty fail", clusterName);
                return RetMessage.createFailMessage("cluster not empty");
            }

            clusterService.deleteCluster(clusterName);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[deleteCluster]", e);
            return RetMessage.createFailMessage(e.getMessage());
        } finally {
            logger.info("[deleteCluster][end]");
        }
    }

    @PutMapping(value = "/cluster")
    public RetMessage updateCluster(@RequestBody ClusterCreateInfo clusterInfo) {
        try {
            Long orgId = clusterInfo.getOrganizationId() == null
                ? null : this.getOrganizationTbl(clusterInfo).getId();
            ClusterUpdateDTO updateDTO = new ClusterUpdateDTO(clusterInfo, orgId);
            String ret = clusterService.updateCluster(updateDTO);
            return RetMessage.createSuccessMessage(ret);
        } catch (Exception e) {
            logger.error("[updateCluster]Failed", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @PutMapping(value = "/clusters")
    public RetMessage updateClusters(@RequestBody List<ClusterCreateInfo> clusterInfos) {
        for(ClusterCreateInfo clusterCreateInfo : clusterInfos) {
            RetMessage retMessage = updateCluster(clusterCreateInfo);
            if(ObjectUtils.equals(retMessage.getState(), RetMessage.FAIL_STATE))
                return retMessage;
        }
        return RetMessage.createSuccessMessage();
    }

    @GetMapping(value = "/cluster/" + CLUSTER_NAME_PATH_VARIABLE)
    public ClusterCreateInfo getCluster(@PathVariable String clusterName) {
        logger.info("[getCluster]{}", clusterName);

        ClusterDTO clusterDTO = clusterService.getCluster(clusterName);
        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo(clusterDTO);

        return transform(clusterCreateInfo, DC_TRANSFORM_DIRECTION.INNER_TO_OUTER);
    }

    @GetMapping(value = "/clusters")
    public List<ClusterCreateInfo> getClusters(@RequestParam(required=false, defaultValue = "one_way", name = "type") String clusterType) throws CheckFailException {
        logger.info("[getClusters]clusterType-{}", clusterType);
        if (!ClusterType.isTypeValidate(clusterType)) {
            throw new CheckFailException("unknown cluster type " + clusterType);
        }

        List<ClusterDTO> clusterDTOList = clusterService.getClusters(clusterType);
        List<ClusterCreateInfo> clusterCreateInfoList = clusterDTOList.stream()
            .map(ClusterCreateInfo::new)
            .collect(Collectors.toList());

        return transformFromInner(clusterCreateInfoList);
    }

    @GetMapping(value = "/clusterWithShards")
    public List<ClusterInfo> getClusterShards(@RequestParam(required=false, defaultValue = "one_way", name = "type") String clusterType) throws CheckFailException {
        logger.info("[clusterWithShards]clusterType-{}", clusterType);
        if (!ClusterType.isTypeValidate(clusterType)) {
            throw new CheckFailException("unknown cluster type " + clusterType);
        }

        List<ClusterDTO> clusterDTOList = clusterService.getClusterWithShards(clusterType);
        List<ClusterInfo> clusterInfos = clusterDTOList.stream().map(ClusterInfo::new).collect(Collectors.toList());
        return clusterInfos;
    }


    @PutMapping(value = "/cluster/exchangename")
    public RetMessage exchangeName(@RequestBody ClusterExchangeNameInfo exchangeNameInfo) {
        ClusterTbl formerClusterTbl = null;
        ClusterTbl latterClusterTbl = null;

        try {
            exchangeNameInfo.check();
            if (!exchangeNameInfo.getToken().equals(config.getOuterClientToken()))
                return RetMessage.createFailMessage("token error");

            formerClusterTbl = clusterService.find(exchangeNameInfo.getFormerClusterName());
            if (formerClusterTbl == null) {
                return RetMessage.createFailMessage("former cluster not exist");
            }

            if (formerClusterTbl.getId() != exchangeNameInfo.getFormerClusterId()) {
                return RetMessage.createFailMessage("former cluster id & name not match");
            }

            latterClusterTbl = clusterService.find(exchangeNameInfo.getLatterClusterName());
            if (latterClusterTbl == null) {
                return RetMessage.createFailMessage("latter cluster not exist");
            }

            if (latterClusterTbl.getId() != exchangeNameInfo.getLatterClusterId()) {
                return RetMessage.createFailMessage("latter cluster id & name not match");
            }

            TransactionMonitor.DEFAULT.logTransaction(AbstractController.META_API_TYPE, "exchangename", new Task<Object>() {
                @Override
                public void go() {
                    clusterService.exchangeName(exchangeNameInfo.getFormerClusterId(),
                        exchangeNameInfo.getFormerClusterName(),
                        exchangeNameInfo.getLatterClusterId(),
                        exchangeNameInfo.getLatterClusterName());
                }

                @Override
                public Map<String, Object> getData() {
                    return new HashMap<String, Object>() {{
                        put("formerClusterDbId", exchangeNameInfo.getFormerClusterId());
                        put("formerClusterId", exchangeNameInfo.getFormerClusterName());
                        put("latterClusterDbId", exchangeNameInfo.getLatterClusterId());
                        put("latterClusterId", exchangeNameInfo.getLatterClusterName());
                    }};
                }
            });
        } catch (CheckFailException cfe) {
            logger.error("[clusterExchangeName][checkFail] {}", exchangeNameInfo, cfe);
            return RetMessage.createFailMessage(cfe.getMessage());
        } catch (Exception e) {
            logger.error("[clusterExchangeName][fail] {}", exchangeNameInfo, e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }

    @PutMapping("/cluster/region/exchange")
    public RetMessage exchangeClusterRegion(@RequestBody ClusterRegionExchangeInfo exchangeInfo) {
        try {
            exchangeInfo.check();

            TransactionMonitor.DEFAULT.logTransaction(AbstractController.META_API_TYPE, "exchangeClusterRegion",
                new Task() {
                    @Override
                    public void go() {
                        clusterService.exchangeRegion(exchangeInfo.getFormerClusterId(),
                            exchangeInfo.getFormerClusterName(),
                            exchangeInfo.getLatterClusterId(),
                            exchangeInfo.getLatterClusterName(),
                            exchangeInfo.getRegionName());
                    }

                    @Override
                    public Map<String, Object> getData() {
                        return new HashMap<String, Object>() {{
                            put("formerClusterDbId", exchangeInfo.getFormerClusterId());
                            put("formerClusterId", exchangeInfo.getFormerClusterName());
                            put("latterClusterDbId", exchangeInfo.getLatterClusterId());
                            put("latterClusterId", exchangeInfo.getLatterClusterName());
                            put("regionName", exchangeInfo.getRegionName());
                        }};
                    }
                });
        } catch (CheckFailException cfe) {
            logger.error("[exchangeClusterRegion][checkFail] {}", exchangeInfo, cfe);
            return RetMessage.createFailMessage(cfe.getMessage());
        } catch (Exception e) {
            logger.error("[exchangeClusterRegion][fail] {}", exchangeInfo, e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();

    }

    @PostMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/azGroup")
    public RetMessage upgradeAzGroup(@PathVariable String clusterName) {
        clusterService.upgradeAzGroup(clusterName);
        return RetMessage.createSuccessMessage();
    }

    @DeleteMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/azGroup")
    public RetMessage downgradeAzGroup(@PathVariable String clusterName) {
        clusterService.downgradeAzGroup(clusterName);
        return RetMessage.createSuccessMessage();
    }

    @PostMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}")
    public RetMessage bindDc(@PathVariable String clusterName, @PathVariable String dcName,
        @RequestBody(required = false) DcDetailInfo dcDetailInfo) {
        logger.info("[bindDc]{},{}", clusterName, dcName);
        if (dcDetailInfo != null) {
            logger.warn("[bindDc]cluster: {}, dc: {}, with deprecated dcDetail-{}",
                clusterName, dcName, dcDetailInfo);
            Cat.logEvent(AbstractController.META_API_TYPE, "[BindDc]-With DcDetail", Message.SUCCESS,
                String.format("cluster=%s&dc=%s&dcDetail=%s", clusterName, dcName, dcDetailInfo));
        }
        try {
            clusterService.bindDc(clusterName, dcName);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[bindDc]cluster: {}, dc: {} failed, error: {}", clusterName, dcName, e, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @PostMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/regions/" + REGION_NAME_PATH_VARIABLE + "/azs/" + AZ_NAME_PATH_VARIABLE)
    public RetMessage bindRegionAz(@PathVariable String clusterName, @PathVariable String regionName,
        @PathVariable String azName) {
        try {
            clusterService.bindRegionAz(clusterName, regionName, azName);
        } catch (Exception e) {
            logger.error("cluster-{} bind az-{} to region-{} error-{}", clusterName, azName, regionName, e, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
        return RetMessage.createSuccessMessage();
    }


    @DeleteMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}")
    public RetMessage unbindDc(@PathVariable String clusterName, @PathVariable String dcName) {
        logger.info("[unbindDc]{}, {}", clusterName, dcName);
        try {
            List<RedisTbl> redises = redisService.findAllRedisesByDcClusterName(dcName, clusterName);
            if (!redises.isEmpty()) {
                logger.info("[unbindDc][{}] check empty fail for dc {}", clusterName, dcName);
                return RetMessage.createFailMessage("cluster not empty in dc " + dcName);
            }

            clusterService.unbindAz(clusterName, dcName);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[unbindDc]cluster: {}, dc: {} failed, error: {}", clusterName, dcName, e, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }


    private OrganizationTbl getOrganizationTbl(ClusterCreateInfo clusterCreateInfo) {
        Long organizationId = clusterCreateInfo.getOrganizationId();
        if(organizationId == null) {
            throw new IllegalStateException("organizationId is required");
        }
        OrganizationTbl organizationTbl = organizationService.getOrganizationTblByCMSOrganiztionId(organizationId);
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
        if (!CollectionUtils.isEmpty(dcs)) {
            List<String> trans = dcs.stream().map(dc -> {
                String transfer = direction.transform(dc);
                if (!Objects.equals(transfer, dc)) {
                    logger.info("[transform]{}->{}", dc, transfer);
                }
                return transfer;
            }).collect(Collectors.toList());
            clusterCreateInfo.setDcs(trans);
        }

        List<RegionInfo> regions = clusterCreateInfo.getRegions();
        if (!CollectionUtils.isEmpty(regions)) {
            for (RegionInfo region : regions) {
                String transActiveAz = direction.transform(region.getActiveAz());
                if (!Objects.equals(transActiveAz, region.getActiveAz())) {
                    logger.info("[transform region active az]{}->{}", region.getActiveAz(), transActiveAz);
                }
                region.setActiveAz(transActiveAz);

                List<String> transAzs = region.getAzs().stream().map(az -> {
                    String transfer = direction.transform(az);
                    if (!Objects.equals(transfer, az)) {
                        logger.info("[transform az]{}->{}", az, transfer);
                    }
                    return transfer;
                }).collect(Collectors.toList());
                region.setAzs(transAzs);
            }
        }

        return clusterCreateInfo;
    }

    private List<ClusterCreateInfo> transformFromInner(List<ClusterCreateInfo> source) {
        List<ClusterCreateInfo> results = new LinkedList<>();
        source.forEach(clusterCreateInfo -> results.add(transform(clusterCreateInfo, DC_TRANSFORM_DIRECTION.INNER_TO_OUTER)));
        return results;
    }

    @VisibleForTesting
    void setConfig(ConsoleConfig config) {
        this.config = config;
    }

}
