package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class ClusterMetaClone implements InnerMetaClone<ClusterMeta> {

    @Override
    public ClusterMeta clone(ClusterMeta o) {
        ClusterMeta clone = new ClusterMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);

        if (null != o.getSources()) {
            for (SourceMeta sourceMeta: o.getSources()) {
                SourceMeta cloneSource = MetaCloneFacade.INSTANCE.clone(sourceMeta);
                clone.addSource(cloneSource);
            }
        }

        if (null != o.getShards()) {
            for (ShardMeta shardMeta: o.getShards().values()) {
                ShardMeta cloneShard = MetaCloneFacade.INSTANCE.clone(shardMeta);
                clone.addShard(cloneShard);
            }
        }

        return clone;
    }
}
