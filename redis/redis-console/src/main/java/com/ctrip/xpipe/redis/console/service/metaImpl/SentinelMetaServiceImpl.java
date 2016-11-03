package com.ctrip.xpipe.redis.console.service.metaImpl;

import org.springframework.stereotype.Service;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.meta.SentinelMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("setinelMetaService")
public class SentinelMetaServiceImpl implements SentinelMetaService {

	@Override
	public SentinelMeta encodeSetinelMeta(SetinelTbl setinel, DcMeta dcMeta) {
		SentinelMeta setinelMeta = new SentinelMeta();
		
		if(null != setinel) {
			setinelMeta.setId(setinel.getSetinelId());
			setinelMeta.setAddress(setinel.getSetinelAddress());
			setinelMeta.setParent(dcMeta);
		}

		return setinelMeta;
	}

}
