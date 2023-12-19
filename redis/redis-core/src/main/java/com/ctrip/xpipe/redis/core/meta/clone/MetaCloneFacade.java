package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.BaseEntity;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class MetaCloneFacade {

    private Map<Class, InnerMetaClone> innerClones;

    public static final MetaCloneFacade INSTANCE = new MetaCloneFacade();

    private MetaCloneFacade() {
        this.innerClones = new HashMap<>();

        this.innerClones.put(RedisMeta.class, new RedisMetaClone());
        this.innerClones.put(KeeperMeta.class, new KeeperMetaClone());
        this.innerClones.put(ApplierMeta.class, new ApplierMetaClone());
        this.innerClones.put(KeeperContainerMeta.class, new KeeperContainerMetaClone());
        this.innerClones.put(ApplierContainerMeta.class, new ApplierContainerMetaClone());
        this.innerClones.put(MetaServerMeta.class, new MetaServerMetaClone());
        this.innerClones.put(ZkServerMeta.class, new ZkServerMetaClone());

        this.innerClones.put(AzMeta.class, new AzMetaClone());
        this.innerClones.put(RouteMeta.class, new RouteMetaClone());
        this.innerClones.put(SentinelMeta.class, new SentinelMetaClone());
        this.innerClones.put(RedisCheckRuleMeta.class, new RedisCheckRuleMetaClone());
        this.innerClones.put(SourceMeta.class, new SourceMetaClone());

        this.innerClones.put(XpipeMeta.class, new XpipeMetaClone());
        this.innerClones.put(DcMeta.class, new DcMetaClone());
        this.innerClones.put(ClusterMeta.class, new ClusterMetaClone());
        this.innerClones.put(ShardMeta.class, new ShardMetaClone());
    }

    public <T extends BaseEntity> T clone(T o) {
        if (null == o) return null;

        if (innerClones.containsKey(o.getClass())) {
            return (T) innerClones.get(o.getClass()).clone(o);
        } else {
            throw new UnsupportedOperationException("no handler for class " + o.getClass().getSimpleName());
        }
    }

    public <T extends BaseEntity> List<T> cloneList(List<T> objects) {
        if (null == objects) return null;

        List<T> cloneObjects = new ArrayList<>(objects.size());
        for (T o: objects) {
            cloneObjects.add(clone(o));
        }

        return cloneObjects;
    }

}
