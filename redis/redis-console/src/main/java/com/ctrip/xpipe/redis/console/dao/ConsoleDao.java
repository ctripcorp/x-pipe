package com.ctrip.xpipe.redis.console.dao;





import com.ctrip.xpipe.redis.core.dao.MetaDao;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public interface ConsoleDao extends MetaDao, ConsoleUpdateOperation{

	XpipeMeta getXpipeMeta();
	
}
