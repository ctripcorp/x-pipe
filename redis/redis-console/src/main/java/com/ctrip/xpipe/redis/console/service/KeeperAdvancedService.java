package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RedisTbl;

import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public interface KeeperAdvancedService {

    List<RedisTbl> findBestKeepers(String dcName, int beginPort, BiPredicate<String, Integer> keeperGood);

}
