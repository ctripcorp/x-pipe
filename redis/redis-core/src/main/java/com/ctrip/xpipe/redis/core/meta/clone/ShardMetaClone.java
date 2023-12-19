package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class ShardMetaClone implements InnerMetaClone<ShardMeta> {

    @Override
    public ShardMeta clone(ShardMeta o) {
        ShardMeta clone = new ShardMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);

        if (null != o.getAppliers()) {
            for (ApplierMeta applierMeta: o.getAppliers()) {
                ApplierMeta cloneApplier = MetaCloneFacade.INSTANCE.clone(applierMeta);
                clone.addApplier(cloneApplier);
            }
        }

        if (null != o.getKeepers()) {
            for (KeeperMeta keeperMeta: o.getKeepers()) {
                KeeperMeta cloneKeeper = MetaCloneFacade.INSTANCE.clone(keeperMeta);
                clone.addKeeper(cloneKeeper);
            }
        }

        if (null != o.getRedises()) {
            for (RedisMeta redisMeta: o.getRedises()) {
                RedisMeta cloneRedis = MetaCloneFacade.INSTANCE.clone(redisMeta);
                clone.addRedis(cloneRedis);
            }
        }

        return clone;
    }
}
