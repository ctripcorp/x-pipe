package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.query.DalQueryHandler;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
public abstract class AbstractXpipeConsoleDAO {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	public static String DELETED_NAME_SPLIT_TAG = "-";
	
	protected DalQueryHandler queryHandler = new DalQueryHandler();

	protected String generateDeletedName(String originName) {
		StringBuilder sb = new StringBuilder(XPipeConsoleConstant.MAX_NAME_SIZE);
		sb.append(DataModifiedTimeGenerator.generateModifiedTime());
		sb.append(DELETED_NAME_SPLIT_TAG);
		sb.append(originName);
		
		String result = sb.toString();
		return result.length() <= XPipeConsoleConstant.MAX_NAME_SIZE ? result : result.substring(0, XPipeConsoleConstant.MAX_NAME_SIZE - 1);
	}

}
