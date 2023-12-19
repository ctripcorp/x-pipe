package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.AzMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class AzMetaClone implements InnerMetaClone<AzMeta> {

    @Override
    public AzMeta clone(AzMeta o) {
        AzMeta clone = new AzMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }

}
