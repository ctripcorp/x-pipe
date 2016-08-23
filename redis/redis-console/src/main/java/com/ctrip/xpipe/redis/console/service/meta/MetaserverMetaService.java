package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface MetaserverMetaService {
	public MetaServerMeta encodeMetaserver(MetaserverTbl metaserver, DcMeta dcMeta);
}
