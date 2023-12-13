package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class SourceMetaClone implements InnerMetaClone<SourceMeta> {

    @Override
    public SourceMeta clone(SourceMeta o) {
        SourceMeta clone = new SourceMeta();
        clone.mergeAttributes(o);

        if (null != o.getShards()) {
            for (ShardMeta shardMeta: o.getShards().values()) {
                ShardMeta cloneShard = MetaCloneFacade.INSTANCE.clone(shardMeta);
                clone.addShard(cloneShard);
            }
        }
        return clone;
    }
}
