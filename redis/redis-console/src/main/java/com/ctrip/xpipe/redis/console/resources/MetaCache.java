package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.core.entity.DcMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public interface MetaCache {

    List<DcMeta> getDcMetas();
}
