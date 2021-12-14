package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *
 *         Sep 9, 2016
 */
@Component
public class DefaultKeeperActiveElectAlgorithmManager implements KeeperActiveElectAlgorithmManager {

	private static Logger logger = LoggerFactory.getLogger(DefaultKeeperActiveElectAlgorithmManager.class);

	@SuppressWarnings("unused")
	@Autowired
	private DcMetaCache dcMetaCache;

	@Override
	public KeeperActiveElectAlgorithm get(Long clusterDbId, Long shardDbId) {

		logger.debug("[get][active dc, use default]");
		return new DefaultKeeperActiveElectAlgorithm();
	}

	public void setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
	}

}
