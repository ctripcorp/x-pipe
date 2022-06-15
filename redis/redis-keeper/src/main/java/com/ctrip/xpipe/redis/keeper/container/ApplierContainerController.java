package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.entity.ApplierInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.spring.AbstractController;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/6/9
 */
@RestController
@RequestMapping("/appliers")
public class ApplierContainerController extends AbstractController {

    @Autowired
    private ApplierContainerService applierContainerService;

    @RequestMapping(method = RequestMethod.POST)
    public void add(@RequestBody ApplierTransMeta applierTransMeta) {

        logger.info("[add]{}", applierTransMeta);
        applierContainerService.add(applierTransMeta);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public void addOrStart(@RequestBody ApplierTransMeta applierTransMeta) {

        logger.info("[addOrStart]{}", applierTransMeta);
        applierContainerService.addOrStart(applierTransMeta);
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<ApplierInstanceMeta> list() {
        logger.info("[list]");
        List<ApplierInstanceMeta> appliers = FluentIterable.from(applierContainerService.list()).transform(
                new Function<ApplierServer, ApplierInstanceMeta>() {
                    @Override
                    public ApplierInstanceMeta apply(ApplierServer server) {
                        return server.getApplierInstanceMeta();
                    }
                }).toList();
        return appliers;
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.DELETE)
    public void remove(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody ApplierTransMeta applierTransMeta) {

        logger.info("[remove]{},{},{}", clusterName, shardName, applierTransMeta);
        applierContainerService.remove(ClusterId.from(applierTransMeta.getClusterDbId()), ShardId.from(applierTransMeta.getShardDbId()));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/start", method = RequestMethod.PUT)
    public void start(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody ApplierTransMeta applierTransMeta) {

        logger.info("[start]{},{},{}", clusterName, shardName, applierTransMeta);
        applierContainerService.start(ClusterId.from(applierTransMeta.getClusterDbId()), ShardId.from(applierTransMeta.getShardDbId()));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/stop", method = RequestMethod.PUT)
    public void stop(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody ApplierTransMeta applierTransMeta) {

        logger.info("[stop]{},{},{}", clusterName, shardName, applierTransMeta);
        applierContainerService.stop(ClusterId.from(applierTransMeta.getClusterDbId()), ShardId.from(applierTransMeta.getShardDbId()));
    }

}
