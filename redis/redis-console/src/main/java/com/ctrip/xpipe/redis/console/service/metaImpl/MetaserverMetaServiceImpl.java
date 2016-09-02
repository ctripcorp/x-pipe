package com.ctrip.xpipe.redis.console.service.metaImpl;

import org.springframework.stereotype.Service;

import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.service.meta.MetaserverMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service("metaserverMetaService")
public class MetaserverMetaServiceImpl implements MetaserverMetaService {

	@Override
	public MetaServerMeta encodeMetaserver(MetaserverTbl metaserver, DcMeta dcMeta) {
		MetaServerMeta metaserverMeta = new MetaServerMeta();
		
		if(null != metaserver) {
			metaserverMeta.setIp(metaserver.getMetaserverIp());
			metaserverMeta.setPort(metaserver.getMetaserverPort());
			if (metaserver.getMetaserverRole().equals("master")) {
				metaserverMeta.setMaster(true);
			} else {
				metaserverMeta.setMaster(false);
			}
		}
		metaserverMeta.setParent(dcMeta);
		
		return metaserverMeta;
	}

}
