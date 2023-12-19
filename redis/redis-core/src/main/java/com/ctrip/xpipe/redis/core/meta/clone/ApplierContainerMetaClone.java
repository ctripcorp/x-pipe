package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/14
 */
public class ApplierContainerMetaClone implements InnerMetaClone<ApplierContainerMeta> {

    @Override
    public ApplierContainerMeta clone(ApplierContainerMeta o) {
        ApplierContainerMeta clone = new ApplierContainerMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
