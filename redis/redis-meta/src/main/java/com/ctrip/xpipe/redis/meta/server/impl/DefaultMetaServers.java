package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.AbstractClusterServers;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@Component
public class DefaultMetaServers extends AbstractClusterServers<MetaServer>{

}
