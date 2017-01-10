package com.ctrip.xpipe.migration;

import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.migration.MigrationPublishService;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public abstract class AbstractMigrationPublishService implements MigrationPublishService {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}
}
