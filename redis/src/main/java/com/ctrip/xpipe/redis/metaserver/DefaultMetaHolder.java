/**
 * 
 */
package com.ctrip.xpipe.redis.metaserver;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.keeper.entity.XpipeMeta;
import com.ctrip.xpipe.redis.keeper.transform.DefaultSaxParser;

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
		meta = DefaultSaxParser.parse(this.getClass().getResourceAsStream("/metaserver.xml"));
	}

	@Override
	public XpipeMeta getMeta() {
		return meta;
	}

}
