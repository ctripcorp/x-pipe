package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.SentinelMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import org.springframework.stereotype.Service;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service
public class SentinelMetaServiceImpl extends AbstractMetaService implements SentinelMetaService {

	@Override
	public SentinelMeta encodeSetinelMeta(SetinelTbl sentinel, DcMeta dcMeta) {
		SentinelMeta sentinelMeta = new SentinelMeta();
		
		if(null != sentinel) {
			sentinelMeta.setId(sentinel.getSetinelId());
			sentinelMeta.setAddress(sentinel.getSetinelAddress());
			sentinelMeta.setParent(dcMeta);
		}

		return sentinelMeta;
	}

}
