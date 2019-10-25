package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.IpUtils;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@RestController
@RequestMapping("/keepers")
public class KeeperContainerController extends AbstractController {
    @Autowired
    private KeeperContainerService keeperContainerService;

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
    public void remove(@PathVariable String clusterName, @PathVariable String shardName) {

        logger.info("[remove]{},{}", clusterName, shardName);
        keeperContainerService.remove(clusterName, shardName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/start", method = RequestMethod.PUT)
    public void start(@PathVariable String clusterName, @PathVariable String shardName) {

        logger.info("[start]{},{}", clusterName, shardName);
        keeperContainerService.start(clusterName, shardName);
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/shards/" + SHARD_NAME_PATH_VARIABLE + "/stop", method = RequestMethod.PUT)
    public void stop(@PathVariable String clusterName, @PathVariable String shardName) {

        logger.info("[stop]{},{}", clusterName, shardName);
        keeperContainerService.stop(clusterName, shardName);
    }

}
