package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface DcMetaService {
	
	DcMeta getDcMeta(String dcName);
	
}
