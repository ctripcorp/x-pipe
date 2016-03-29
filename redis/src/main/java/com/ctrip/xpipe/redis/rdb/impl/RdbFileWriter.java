package com.ctrip.xpipe.redis.rdb.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.rdb.RdbWriter;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午4:59:24
 */
public class RdbFileWriter extends RdbWriter{

	private String rdbFileName;
	
	private OutputStream ous;
	
	public RdbFileWriter(String rdbFileName) {
		this.rdbFileName = rdbFileName;
	}
	

	@Override
	public void doBeginWrite() {
		
		try {
			ous = new FileOutputStream(new File(rdbFileName));
		} catch (FileNotFoundException e) {
			throw new RedisRuntimeException("[beginWrite]" + rdbFileName, e);
		}
	}

	@Override
	public void write(int b) {
		try {
			ous.write(b);
		} catch (IOException e) {
			logger.error("[write]", e);
		}
	}

	@Override
	public void doEndWrite() {
		if(ous != null){
			try {
				ous.close();
			} catch (IOException e) {
				logger.error("[endWrite]", e);
			}
		}
	}
	
	@Override
	public String toString() {
		
		return getClass().getSimpleName() + ",rdbFile:" + rdbFileName;
	}

}
