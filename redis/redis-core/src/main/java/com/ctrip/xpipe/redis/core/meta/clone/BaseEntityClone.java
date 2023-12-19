package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.BaseEntity;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class BaseEntityClone<T extends BaseEntity> implements InnerMetaClone<T> {

    @Override
    public T clone(T o) {
        try {
            T clone = (T)o.getClass().newInstance();
            clone.mergeAttributes(o);
            return clone;
        } catch (InstantiationException|IllegalAccessException e) {
            throw new MetaCloneException(o.getClass().getSimpleName(), e);
        }
    }

}
