package com.ctrip.xpipe.redis.meta.server.dcchange;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface NewMasterChooser {
	
	RedisMeta choose(List<RedisMeta> redises);

}
