package com.ctrip.xpipe.api.pool;

import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.ReturnObjectException;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public interface SimpleObjectPool<T> {
	
	T borrowObject() throws BorrowObjectException;
	
	void returnObject(T obj) throws ReturnObjectException;

	void clear() throws ObjectPoolException;
	
	String desc();

}
