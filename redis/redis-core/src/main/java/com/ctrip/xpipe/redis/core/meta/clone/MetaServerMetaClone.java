package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/14
 */
public class MetaServerMetaClone implements InnerMetaClone<MetaServerMeta> {

    @Override
    public MetaServerMeta clone(MetaServerMeta o) {
        MetaServerMeta clone = new MetaServerMeta();
        clone.mergeAttributes(o);
        return clone;
    }
}
