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

	public static String MEMORY_META_SERVER_DAO_KEY = "memory_meta_server_dao_file";
	
	private String fileName;

	public MemoryMetaServerDao(){
	}

	public MemoryMetaServerDao(String fileName){
		this.fileName = fileName;
	}

	@Override
	protected MetaDao loadMetaDao() {

		if(fileName == null){
			fileName = System.getProperty(MEMORY_META_SERVER_DAO_KEY, "memory_meta_server_dao_file.xml");
		}
		
		if(fileName != null){
			return new DefaultMemoryMetaDao(fileName);
		}
		return new DefaultMemoryMetaDao(fileName);
	}

}
