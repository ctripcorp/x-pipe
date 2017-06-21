package com.ctrip.xpipe.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.migration.OuterClientService;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public abstract class AbstractOuterClientService implements OuterClientService {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}
}
