package com.ctrip.xpipe.redis.console.cache;

import com.ctrip.xpipe.redis.console.model.DcTbl;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public interface DcCache {

    DcTbl find(String dcName);
    DcTbl find(long dcId);

}
