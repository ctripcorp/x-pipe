package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/14
 */
public class ApplierMetaClone implements InnerMetaClone<ApplierMeta> {

    @Override
    public ApplierMeta clone(ApplierMeta o) {
        ApplierMeta clone = new ApplierMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
