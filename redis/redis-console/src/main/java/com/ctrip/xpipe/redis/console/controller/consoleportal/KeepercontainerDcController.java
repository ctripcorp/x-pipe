package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;


/**
 * @author shyin
 *         <p>
 *         Aug 22, 2016
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class KeepercontainerDcController extends AbstractConsoleController {

    @Autowired
    private KeepercontainerService keepercontainerService;

    @Autowired
    private KeeperAdvancedService keeperAdvancedService;


    @RequestMapping(value = "/dcs/{dcName}/activekeepercontainers", method = RequestMethod.GET)
    public List<KeepercontainerTbl> findKeeperContainer(@PathVariable String dcName) {
        return keepercontainerService.findAllActiveByDcName(dcName);
    }


    @RequestMapping(value = "/dcs/{dcName}/availablekeepers", method = RequestMethod.POST)
    public List<RedisTbl> findAvailableKeepers(@PathVariable String dcName,
                                               @RequestBody(required = false) ShardModel shardModel) {

        logger.debug("[findAvailableKeepers]{}, {}", dcName, shardModel);

        int beginPort = findBeginPort(shardModel);

        List<KeeperBasicInfo> bestKeepers = keeperAdvancedService.findBestKeepers(dcName, beginPort, (ip, port) -> {

            if (shardModel != null && existOnConsole(ip, port, shardModel.getKeepers())) {
                return false;
            }
            return true;
        });

        List<RedisTbl> result = new LinkedList<>();

        bestKeepers.forEach(keeperSelected -> result.add(
                new RedisTbl().setKeepercontainerId(keeperSelected.getKeeperContainerId())
                .setRedisIp(keeperSelected.getHost())
                .setRedisPort(keeperSelected.getPort())
        ));
        return result;
    }

    private boolean existOnConsole(String keepercontainerIp, int port, List<RedisTbl> keepers) {

        for (RedisTbl redisTbl : keepers) {
            if (redisTbl.getRedisIp().equalsIgnoreCase(keepercontainerIp)
                    && redisTbl.getRedisPort() == port) {
                return true;
            }
        }
        return false;
    }

    private int findBeginPort(ShardModel shardModel) {

        int port = RedisProtocol.REDIS_PORT_DEFAULT;
        if (shardModel != null && shardModel.getRedises().size() > 0) {
            port = shardModel.getRedises().get(0).getRedisPort();
        }
        return port;
    }
}
