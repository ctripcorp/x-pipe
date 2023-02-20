package com.ctrip.xpipe.redis.console.service.meta;

import com.ctrip.xpipe.redis.core.entity.DcMeta;

import java.util.Map;
import java.util.Set;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public interface DcMetaService {
	
	DcMeta getDcMeta(String dcName) throws Exception;

	DcMeta getDcMeta(String dcName, Set<String> allowTypes) throws Exception;

	Map<String, DcMeta> getAllDcMetas() throws Exception;
	
}
