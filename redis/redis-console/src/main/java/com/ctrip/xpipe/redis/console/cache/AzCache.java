package com.ctrip.xpipe.redis.console.cache;

import com.ctrip.xpipe.redis.console.model.AzTbl;

/**
 * @author yihaohuang
 */
public interface AzCache {

    AzTbl find(String azName);

    AzTbl find(long azId);

    Long findId(String azName);

}
