package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.query.DalQueryHandler;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
public abstract class AbstractXpipeConsoleDAO {
	public static String DELETED_NAME_SPLIT_TAG = "-";
	
	protected DalQueryHandler queryHandler = new DalQueryHandler();

	protected String generateDeletedName(String originName) {
		StringBuilder sb = new StringBuilder(XpipeConsoleConstant.MAX_NAME_SIZE);
		sb.append(DataModifiedTimeGenerator.generateModifiedTime());
		sb.append(DELETED_NAME_SPLIT_TAG);
		sb.append(originName);
		
		String result = sb.toString();
		return result.length() <= XpipeConsoleConstant.MAX_NAME_SIZE ? result : result.substring(0, XpipeConsoleConstant.MAX_NAME_SIZE - 1);
	}

}
