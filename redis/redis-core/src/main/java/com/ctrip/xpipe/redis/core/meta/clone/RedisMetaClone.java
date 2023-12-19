package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/14
 */
public class RedisMetaClone implements InnerMetaClone<RedisMeta> {

    @Override
    public RedisMeta clone(RedisMeta o) {
        RedisMeta clone = new RedisMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
