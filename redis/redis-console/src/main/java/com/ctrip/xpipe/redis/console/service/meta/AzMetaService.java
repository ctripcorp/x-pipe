package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.core.entity.AzMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

public interface AzMetaService {
    AzMeta encodeAzMeta(AzTbl azTbl, DcMeta dcMeta);
}
