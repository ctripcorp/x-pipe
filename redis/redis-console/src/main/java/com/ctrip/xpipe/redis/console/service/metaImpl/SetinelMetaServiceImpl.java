package com.ctrip.xpipe.redis.console.service.metaImpl;

import org.springframework.stereotype.Service;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.meta.SetinelMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SetinelMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("setinelMetaService")
public class SetinelMetaServiceImpl implements SetinelMetaService {

	@Override
	public SetinelMeta encodeSetinelMeta(SetinelTbl setinel, DcMeta dcMeta) {
		SetinelMeta setinelMeta = new SetinelMeta();
		
		if(null != setinel) {
			setinelMeta.setId(setinel.getSetinelId());
			setinelMeta.setAddress(setinel.getSetinelAddress());
			setinelMeta.setParent(dcMeta);
		}

		return setinelMeta;
	}

}
