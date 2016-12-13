package com.ctrip.xpipe.redis.meta.server.dcchange.exception;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.exception.MetaServerRuntimeException;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
public class ChooseNewMasterFailException extends MetaServerRuntimeException{
	
	private static final long serialVersionUID = 1L;
	
	private List<RedisMeta> redises;
	
	public ChooseNewMasterFailException(List<RedisMeta> redises) {
		super("can not choose master from redises:" + redises);
	}

	public List<RedisMeta> getRedises() {
		return new LinkedList<>(redises);
	}
}
