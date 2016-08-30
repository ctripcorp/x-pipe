package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
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
public class KeeperContainerController extends AbstractController{
    @Autowired
    private KeeperContainerService keeperContainerService;

    @RequestMapping(method = RequestMethod.POST)
    public void add(@RequestBody KeeperTransMeta keeperTransMeta) {
    	
    	logger.info("[add]{}", keeperTransMeta);
        keeperContainerService.add(keeperTransMeta);
    }

    @RequestMapping(value = "/clusters/{cluster}/shards/{shard}", method = RequestMethod.POST)
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

    @RequestMapping(value = "/clusters/{cluster}/shards/{shard}", method = RequestMethod.DELETE)
    public void remove(@PathVariable String cluster, @PathVariable String shard) {
    	
    	logger.info("[remove]{},{}", cluster, shard);
        keeperContainerService.remove(cluster, shard);
    }

    @RequestMapping(value = "/clusters/{cluster}/shards/{shard}/start", method = RequestMethod.PUT)
    public void start(@PathVariable String cluster, @PathVariable String shard) {
    	
    	logger.info("[start]{},{}", cluster, shard);
        keeperContainerService.start(cluster, shard);
    }

    @RequestMapping(value = "/clusters/{cluster}/shards/{shard}/stop", method = RequestMethod.PUT)
    public void stop(@PathVariable String cluster, @PathVariable String shard) {
    	
    	logger.info("[stop]{},{}", cluster, shard);
        keeperContainerService.stop(cluster, shard);
    }
}
