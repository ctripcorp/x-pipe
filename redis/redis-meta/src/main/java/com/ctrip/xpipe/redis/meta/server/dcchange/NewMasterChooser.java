package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface NewMasterChooser {
	
	RedisMeta choose(List<RedisMeta> redises);

}
