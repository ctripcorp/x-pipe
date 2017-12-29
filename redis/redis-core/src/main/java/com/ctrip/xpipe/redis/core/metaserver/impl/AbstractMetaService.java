package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.util.List;

/**
 * @author wenchao.meng
 *
 *         Sep 5, 2016
 */
public abstract class AbstractMetaService implements MetaServerService {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	public static int DEFAULT_RETRY_TIMES = Integer.parseInt(System.getProperty("metaserver.retryTimes", "3"));

	public static int DEFAULT_RETRY_INTERVAL_MILLI = Integer
			.parseInt(System.getProperty("metaserver.retryIntervalMilli", "5"));

	private int retryTimes;

	private int retryIntervalMilli;

	protected RestOperations restTemplate;

	public AbstractMetaService() {
		this(DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI);
	}

	public AbstractMetaService(int retryTimes, int retryIntervalMilli) {

		this.retryTimes = retryTimes;
		this.retryIntervalMilli = retryIntervalMilli;
		this.restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(this.retryTimes, this.retryIntervalMilli);
	}

	protected <T> T pollMetaServer(Function<String, T> fun) {

		List<String> metaServerList = getMetaServerList();

		for (String url : metaServerList) {

			try {
				T result = fun.apply(url);
				if (result != null) {
					return result;
				}
			} catch (Exception e) {
				logger.error("[pollMetaServer][error poll server]{}", url);
			}
		}
		return null;
	}

	protected abstract List<String> getMetaServerList();

	@Override
	public KeeperMeta getActiveKeeper(final String clusterId, final String shardId) {

		return pollMetaServer(new Function<String, KeeperMeta>() {

			@Override
			public KeeperMeta apply(String metaServerAddress) {

				
				String activeKeeperPath = META_SERVER_SERVICE.GET_ACTIVE_KEEPER.getRealPath(metaServerAddress);
				KeeperMeta keeperMeta = restTemplate.getForObject(activeKeeperPath, KeeperMeta.class, clusterId, shardId);
				return keeperMeta;
			}

		});
	}
}
