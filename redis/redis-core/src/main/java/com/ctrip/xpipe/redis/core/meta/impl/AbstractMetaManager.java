package com.ctrip.xpipe.redis.core.meta.impl;


import com.ctrip.xpipe.redis.core.BaseEntity;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public abstract class AbstractMetaManager implements XpipeMetaManager{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	

	protected <T extends BaseEntity> T clone(T obj){
		return MetaCloneFacade.INSTANCE.clone(obj);
	}

	protected <T extends BaseEntity> List<T> cloneList(List<T> objs){
		return MetaCloneFacade.INSTANCE.cloneList(objs);
	}

}
