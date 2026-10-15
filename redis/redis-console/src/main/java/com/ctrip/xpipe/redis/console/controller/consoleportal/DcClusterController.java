package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.DcClusterModel;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class DcClusterController extends AbstractConsoleController {

    @Autowired
    DcClusterService dcClusterService;

    @Autowired
    CrossDcKeeperContainerAddressService keeperContainerAddressService;

    @RequestMapping("/dc-cluster/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}")
    public DcClusterModel findDcClusterModel(@PathVariable String clusterName, @PathVariable String dcName) {
        DcClusterModel dcClusterModel = dcClusterService.findDcClusterModelByClusterAndDc(clusterName, dcName);
        fillContainerKeeperIp(dcName, dcClusterModel);
        return dcClusterModel;
    }

    @RequestMapping("/dc-cluster/clusters/" + CLUSTER_NAME_PATH_VARIABLE)
    public List<DcClusterModel> findDcClusterModels(@PathVariable String clusterName) {
        return dcClusterService.findDcClusterModelsByCluster(clusterName);
    }

    /**
     * 容器 keeper 在 DB 里的 redis_ip 是占位值，这里按 hostname 反查出真实 IP 回填给前端展示。
     * 仅当存在带 hostname 的 keeper 时才发起反查，一次拿到目标机房的全量映射，避免逐实例查询；
     * 反查不到就保持原值，由前端只展示 hostname
     */
    private void fillContainerKeeperIp(String dcName, DcClusterModel dcClusterModel) {
        if (null == dcClusterModel) {
            return;
        }

        List<RedisTbl> containerKeepers = collectContainerKeepers(dcClusterModel);
        if (containerKeepers.isEmpty()) {
            return;
        }

        Map<String, String> hostnameToIp = keeperContainerAddressService.getAllHostnameToIp(dcName);
        if (null == hostnameToIp || hostnameToIp.isEmpty()) {
            logger.warn("[fillContainerKeeperIp] no hostname-ip mapping from dc: {}", dcName);
            return;
        }

        containerKeepers.forEach(keeper -> {
            String ip = hostnameToIp.get(keeper.getHostName());
            if (!StringUtil.isEmpty(ip)) {
                keeper.setRedisIp(ip);
            }
        });
    }

    private List<RedisTbl> collectContainerKeepers(DcClusterModel dcClusterModel) {
        List<RedisTbl> containerKeepers = new ArrayList<>();
        addContainerKeepers(dcClusterModel.getShards(), containerKeepers);
        if (null != dcClusterModel.getSources()) {
            dcClusterModel.getSources().forEach(source -> addContainerKeepers(source.getShards(), containerKeepers));
        }
        return containerKeepers;
    }

    private void addContainerKeepers(List<ShardModel> shards, List<RedisTbl> containerKeepers) {
        if (null == shards) {
            return;
        }
        shards.forEach(shard -> {
            if (null == shard.getKeepers()) {
                return;
            }
            shard.getKeepers().forEach(keeper -> {
                if (!StringUtil.isEmpty(keeper.getHostName())) {
                    containerKeepers.add(keeper);
                }
            });
        });
    }
}
