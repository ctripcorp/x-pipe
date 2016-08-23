package com.ctrip.xpipe.zk;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author wenchao.meng
 *
 * Aug 23, 2016
 */
public class EphemeralNodeCanNotReplaceException extends XpipeRuntimeException{
	
	private static final long serialVersionUID = 1L;

	public EphemeralNodeCanNotReplaceException(String path, byte []zkData, byte[] data){
		super(String.format("path:%s, zk:%s, outs:%s", path, new String(zkData), new String(data)));
		
	}

}
