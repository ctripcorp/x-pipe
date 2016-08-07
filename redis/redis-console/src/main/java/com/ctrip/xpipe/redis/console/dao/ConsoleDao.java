package com.ctrip.xpipe.redis.console.dao;





import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public interface ConsoleDao extends XpipeMetaManager, ConsoleUpdateOperation{

	XpipeMeta getXpipeMeta();
	
}
