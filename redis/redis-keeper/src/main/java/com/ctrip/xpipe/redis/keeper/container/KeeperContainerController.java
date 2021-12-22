package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ratelimit.CompositeLeakyBucket;
import com.ctrip.xpipe.spring.AbstractController;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@RestController
@RequestMapping("/keepers")
public class KeeperContainerController extends AbstractController {
    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private CompositeLeakyBucket leakyBucket;

    @RequestMapping(method = RequestMethod.POST)
    public void add(@RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[add]{}", keeperTransMeta);
        keeperContainerService.add(keeperTransMeta);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public void addOrStart(@RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[addOrStart]{}", keeperTransMeta);
        keeperContainerService.addOrStart(keeperTransMeta);
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<KeeperInstanceMeta> list() {
        logger.info("[list]");
        List<KeeperInstanceMeta> keepers = FluentIterable.from(keeperContainerService.list()).transform(
                new Function<RedisKeeperServer, KeeperInstanceMeta>() {
                    @Override
                    public KeeperInstanceMeta apply(RedisKeeperServer server) {
                        return server.getKeeperInstanceMeta();
                    }
                }).toList();
        return keepers;
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.DELETE)
    public void remove(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[remove]{},{},{}", clusterName, shardName, keeperTransMeta);
        keeperContainerService.remove(ClusterId.from(keeperTransMeta.getClusterDbId()), ShardId.from(keeperTransMeta.getShardDbId()));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/start", method = RequestMethod.PUT)
    public void start(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[start]{},{},{}", clusterName, shardName, keeperTransMeta);
        keeperContainerService.start(ClusterId.from(keeperTransMeta.getClusterDbId()), ShardId.from(keeperTransMeta.getShardDbId()));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/stop", method = RequestMethod.PUT)
    public void stop(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[stop]{},{},{}", clusterName, shardName, keeperTransMeta);
        keeperContainerService.stop(ClusterId.from(keeperTransMeta.getClusterDbId()), ShardId.from(keeperTransMeta.getShardDbId()));
    }

    @RequestMapping(value = "/leakybucket", method = RequestMethod.GET)
    public LeakyBucketInfo getLeakyBucketInfo() {
        return new LeakyBucketInfo(!leakyBucket.isClosed(), leakyBucket.getTotalSize(), leakyBucket.references());
    }

    private class LeakyBucketInfo {
        private boolean open;
        private int size;
        private int availablePermits;

        public LeakyBucketInfo(boolean open, int size, int availablePermits) {
            this.open = open;
            this.size = size;
            this.availablePermits = availablePermits;
        }

        public boolean isOpen() {
            return open;
        }

        public int getSize() {
            return size;
        }

        public int getAvailablePermits() {
            return availablePermits;
        }
    }

}
