package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
@Component
public class DefaultKeeperAdvancedService extends AbstractConsoleService<RedisTblDao> implements KeeperAdvancedService {

    @Autowired
    private KeepercontainerService keepercontainerService;

    @Autowired
    private RedisService redisService;

    @Override
    public List<KeeperSelected> findBestKeepers(String dcName, int beginPort, BiPredicate keeperGood) {
        return findBestKeepers(dcName, beginPort, keeperGood, 2);
    }

    public List<KeeperSelected> findBestKeepers(String dcName, int beginPort, BiPredicate<String, Integer> keeperGood, int returnCount) {

        List<KeepercontainerTbl> keeperCount = keepercontainerService.findKeeperCount(dcName);
        if (keeperCount.size() < returnCount) {
            throw new IllegalStateException("all keepers size:" + keeperCount + ", but we need:" + returnCount);
        }

        List<KeeperSelected> result = new LinkedList<>();

        //find available port
        for (int i = 0; i < returnCount; i++) {

            KeepercontainerTbl keepercontainerTbl = keeperCount.get(i);

            KeeperSelected keeperSelected = new KeeperSelected();

            keeperSelected.setKeeperContainerId(keepercontainerTbl.getKeepercontainerId());
            keeperSelected.setHost(keepercontainerTbl.getKeepercontainerIp());

            int port = findAvailablePort(keepercontainerTbl, beginPort, keeperGood, result);

            keeperSelected.setPort(port);
            result.add(keeperSelected);
        }
        return result;

    }

    private int findAvailablePort(KeepercontainerTbl keepercontainerTbl, int beginPort, BiPredicate<String, Integer> keeperGood, List<KeeperSelected> result) {

        int port = beginPort;
        String ip = keepercontainerTbl.getKeepercontainerIp();

        for (; ; port++) {

            if (alreadySelected(ip, port, result)) {
                continue;
            }

            if (!(keeperGood.test(ip, port))) {
                continue;
            }
            if (existInDb(ip, port)) {
                continue;
            }

            break;
        }

        return port;
    }

    private boolean alreadySelected(String ip, int port, List<KeeperSelected> result) {

        for (KeeperSelected keeperSelected : result) {
            if (keeperSelected.getHost().equalsIgnoreCase(ip) && keeperSelected.getPort() == port) {
                return true;
            }
        }
        return false;
    }

    private boolean existInDb(String keepercontainerIp, int port) {
        return redisService.findWithIpPort(keepercontainerIp, port) != null;
    }
}
