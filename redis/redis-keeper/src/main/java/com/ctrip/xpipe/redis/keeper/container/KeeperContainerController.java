package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ratelimit.CompositeLeakyBucket;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.spring.AbstractController;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

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

    @Autowired
    private SyncRateManager syncRateManager;

    @PostMapping("/limit/totalIO")
    public boolean setTotalIOLimit(@RequestParam int limit) {
        this.syncRateManager.setTotalIOLimit(Math.max(0, limit));
        return true;
    }

    @GetMapping("/limit/totalIO")
    public int getTotalIOLimit() {
        return syncRateManager.getTotalIOLimit();
    }

    @DeleteMapping("/rdb/release")
    public boolean releaseRdb(@RequestBody KeeperTransMeta keeperTransMeta) throws IOException {
        logger.info("[releaseRdb]{}", keeperTransMeta);
        keeperContainerService.releaseRdb(ReplId.from(keeperTransMeta.getReplId()));
        return true;
    }

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

    @GetMapping(value = "/port/{port}")
    public KeeperInstanceMeta infoPort(@PathVariable int port) {
        logger.info("[infoPort] {}", port);
        Optional<RedisKeeperServer> optional = keeperContainerService.list().stream()
                .filter(redisKeeperServer -> redisKeeperServer.getListeningPort() == port)
                .findFirst();

        if (optional.isPresent()) {
            return optional.get().getKeeperInstanceMeta();
        } else {
            return new KeeperInstanceMeta();
        }
    }

    @GetMapping(value = "/disk")
    public KeeperDiskInfo infoDisk() {
        return keeperContainerService.infoDisk();
    }

    @PostMapping("/election/reset" )
    public boolean resetElection(@RequestBody KeeperTransMeta keeperTransMeta) {
        logger.info("[resetElection]{}", keeperTransMeta);
        keeperContainerService.resetElection(ReplId.from(keeperTransMeta.getReplId()));
        return true;
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.DELETE)
    public void remove(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[remove]{},{},{}", clusterName, shardName, keeperTransMeta);
        keeperContainerService.remove(ReplId.from(keeperTransMeta.getReplId()));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/start", method = RequestMethod.PUT)
    public void start(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[start]{},{},{}", clusterName, shardName, keeperTransMeta);
        keeperContainerService.start(ReplId.from(keeperTransMeta.getReplId()));
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/stop", method = RequestMethod.PUT)
    public void stop(@PathVariable String clusterName, @PathVariable String shardName, @RequestBody KeeperTransMeta keeperTransMeta) {

        logger.info("[stop]{},{},{}", clusterName, shardName, keeperTransMeta);
        keeperContainerService.stop(ReplId.from(keeperTransMeta.getReplId()));
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
