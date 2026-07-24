package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.config.ConfigKeyListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author wenchao.meng
 *
 *         Sep 9, 2016
 */
@Component
public class DefaultKeeperActiveElectAlgorithmManager implements KeeperActiveElectAlgorithmManager {

	private static Logger logger = LoggerFactory.getLogger(DefaultKeeperActiveElectAlgorithmManager.class);

	@Autowired
	private DcMetaCache dcMetaCache;

	@Autowired
	private MetaServerConfig metaServerConfig;

	@Autowired
	private KeeperElectReElectService keeperElectReElectService;

	private final ConfigKeyListener strategyChangeListener = new ConfigKeyListener() {
		@Override
		public void onChange(String key, String newValue) {
			if (DefaultMetaServerConfig.KEY_KEEPER_ELECT_STRATEGY.equals(key)) {
				logger.info("[onStrategyChange][{}] re-elect all shards", key);
				keeperElectReElectService.reElectAll();
			}
		}
	};

	@PostConstruct
	public void registerStrategyListener() {
		metaServerConfig.addListener(strategyChangeListener);
	}

	@Override
	public KeeperActiveElectAlgorithm get(Long clusterDbId, Long shardDbId) {
		return new StrategyAwareKeeperActiveElectAlgorithm(metaServerConfig.getKeeperElectStrategy(), dcMetaCache);
	}

	public void setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
	}

	public void setMetaServerConfig(MetaServerConfig metaServerConfig) {
		this.metaServerConfig = metaServerConfig;
	}

	public void setKeeperElectReElectService(KeeperElectReElectService keeperElectReElectService) {
		this.keeperElectReElectService = keeperElectReElectService;
	}
}
