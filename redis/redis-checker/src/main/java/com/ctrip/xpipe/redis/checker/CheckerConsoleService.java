package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public interface CheckerConsoleService {

    XpipeMeta getXpipeMeta(String console, int clusterPartIndex);

}
