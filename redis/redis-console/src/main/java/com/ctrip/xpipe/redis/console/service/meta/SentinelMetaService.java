package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface SentinelMetaService {
	SentinelMeta encodeSetinelMeta(SetinelTbl setinel, DcMeta dcMeta);
}
