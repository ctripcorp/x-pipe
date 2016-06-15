/**
 * 
 */
package com.ctrip.xpipe.redis.metaserver;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.keeper.entity.XpipeMeta;
import com.ctrip.xpipe.redis.keeper.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author marsqing
 *
 *         Jun 12, 2016 3:13:52 PM
 */
@Component
public class DefaultMetaHolder implements MetaHolder {

	private XpipeMeta meta;

	public DefaultMetaHolder() throws Exception {
		// TODO
		FoundationService foundationService = ServicesUtil.getFoundationService();
		meta = DefaultSaxParser.parse(this.getClass().getResourceAsStream(String.format("/metaserver--%s.xml", foundationService.getDataCenter())));
	}

	@Override
	public XpipeMeta getMeta() {
		return meta;
	}

}
