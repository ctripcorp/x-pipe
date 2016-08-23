package com.ctrip.xpipe.redis.console.service.meta;

import java.util.HashMap;

import org.apache.commons.lang3.tuple.Triple;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface DcMetaService {
	
	public DcMeta getDcMeta(String dcName);
	
	public HashMap<Triple<Long, Long, Long>, RedisTbl> loadAllActiveKeepers();
	
}
