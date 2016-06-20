/**
 * 
 */
package com.ctrip.xpipe.redis.meta.server;

import java.io.ByteArrayInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.core.zk.ZkClient;

import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;

/**
 * @author marsqing
 *
 *         Jun 12, 2016 3:13:52 PM
 */
@Component
public class DefaultMetaHolder extends AbstractLifecycle implements MetaHolder {

	private static Logger log = LoggerFactory.getLogger(DefaultMetaHolder.class);

	@Autowired
	private ZkClient zkClient;

	@Autowired
	private MetaServerConfig config;

	private XpipeMeta meta;

	@Override
	protected void doStart() throws Exception {
		String path = config.getZkMetaStoragePath();
		byte[] metaBytes = zkClient.get().getData().forPath(path);
		if (metaBytes == null || metaBytes.length == 0) {
			log.error("Meta not found in zk, path {}", path);
			throw new RuntimeException("meta not found in zk, path " + path);
		} else {
			try {
				meta = DefaultSaxParser.parse(new ByteArrayInputStream(metaBytes));
			} catch (Exception e) {
				log.error("Can not read meta from zk, path {}", path);
				throw new RuntimeException("Can not read meta from zk, path " + path);
			}
		}
	}

	@Override
	public XpipeMeta getMeta() {
		return meta;
	}

}
