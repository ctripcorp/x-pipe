package com.ctrip.xpipe.redis.meta.server.dao.memory;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.core.dao.MetaDao;
import com.ctrip.xpipe.redis.core.dao.memory.DefaultMemoryMetaDao;
import com.ctrip.xpipe.redis.meta.server.dao.AbstractMetaServerDao;
import com.ctrip.xpipe.redis.meta.server.dao.MetaServerDao;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
@Component
public class MemoryMetaServerDao extends AbstractMetaServerDao implements MetaServerDao{

	private String fileName = System.getProperty("memory_meta_server_dao_file", "memory_meta_server_dao_file.xml");

	public MemoryMetaServerDao(){
	}

	public MemoryMetaServerDao(String fileName){
		this.fileName = fileName;
	}

	@Override
	protected MetaDao loadMetaDao() {
		
		if(fileName != null){
			return new DefaultMemoryMetaDao(fileName);
		}
		return new DefaultMemoryMetaDao(fileName);
	}

}
