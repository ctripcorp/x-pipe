package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster;

import com.ctrip.xpipe.tuple.Pair;

/**
 * @author ayq
 * <p>
 * 2022/4/10 13:03
 */
public interface ApplierMasterChooserAlgorithm {

    Pair<String, Integer> choose();

}
