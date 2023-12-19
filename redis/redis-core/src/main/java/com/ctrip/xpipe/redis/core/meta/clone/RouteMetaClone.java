package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class RouteMetaClone implements InnerMetaClone<RouteMeta> {

    @Override
    public RouteMeta clone(RouteMeta o) {
        RouteMeta clone = new RouteMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
