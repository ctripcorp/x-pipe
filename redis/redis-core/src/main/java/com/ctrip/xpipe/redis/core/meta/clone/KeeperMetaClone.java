package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/14
 */
public class KeeperMetaClone implements InnerMetaClone<KeeperMeta> {

    @Override
    public KeeperMeta clone(KeeperMeta o) {
        KeeperMeta clone = new KeeperMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
