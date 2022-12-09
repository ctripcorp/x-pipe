package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.AppliercontainerTbl;
import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author ayq
 * <p>
 * 2022/6/15 17:25
 */
public interface AppliercontainerMetaService {
    ApplierContainerMeta encodeAppliercontainerMeta(AppliercontainerTbl appliercontainer, DcMeta dcMeta);
}
