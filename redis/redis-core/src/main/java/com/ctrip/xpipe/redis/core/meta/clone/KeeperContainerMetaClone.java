package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/14
 */
public class KeeperContainerMetaClone implements InnerMetaClone<KeeperContainerMeta> {

    @Override
    public KeeperContainerMeta clone(KeeperContainerMeta o) {
        KeeperContainerMeta clone = new KeeperContainerMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
