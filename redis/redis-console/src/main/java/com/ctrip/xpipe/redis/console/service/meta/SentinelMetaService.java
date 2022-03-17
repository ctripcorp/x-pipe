package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface SentinelMetaService {
	SentinelMeta encodeSetinelMeta(SentinelGroupModel sentinelGroupModel, DcMeta dcMeta);
}
