package com.ctrip.xpipe.redis.core.meta.impl;


import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public abstract class AbstractMetaManager implements XpipeMetaManager{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	

	protected <T extends Serializable> T clone(T obj){
		return MetaClone.clone(obj);
	}

}
