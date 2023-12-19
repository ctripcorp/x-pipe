package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class SentinelMetaClone implements InnerMetaClone<SentinelMeta> {

    @Override
    public SentinelMeta clone(SentinelMeta o) {
        SentinelMeta clone = new SentinelMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
