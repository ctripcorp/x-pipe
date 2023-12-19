package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/14
 */
public class ZkServerMetaClone implements InnerMetaClone<ZkServerMeta> {

    @Override
    public ZkServerMeta clone(ZkServerMeta o) {
        ZkServerMeta clone = new ZkServerMeta();
        clone.mergeAttributes(o);
        return clone;
    }
}
