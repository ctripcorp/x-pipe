package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public interface MetaOperation {
	
	void update(String meta) throws Exception;
	
	void update(XpipeMeta xpipeMeta) throws Exception;

}
