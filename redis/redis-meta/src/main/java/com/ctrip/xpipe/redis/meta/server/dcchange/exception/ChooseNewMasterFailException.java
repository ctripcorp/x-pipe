package com.ctrip.xpipe.redis.meta.server.dcchange.exception;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.meta.server.exception.MetaServerRuntimeException;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
public class ChooseNewMasterFailException extends MetaServerRuntimeException{
	
	private static final long serialVersionUID = 1L;
	
	private List<RedisMeta> redises;
	private List<RedisMeta> masters;

	private ChooseNewMasterFailException(String reason, List<RedisMeta> masters, List<RedisMeta> redises) {
		super(String.format("can not choose master from redises:%s", reason));
		this.masters = masters;
		this.redises = redises;
	}

	public List<RedisMeta> getRedises() {
		return new LinkedList<>(redises);
	}

	public List<RedisMeta> getMasters() {
		return new LinkedList<>(masters);
	}

	public static ChooseNewMasterFailException noAliveServer(List<RedisMeta> allServers){
		return new ChooseNewMasterFailException("noAliveServer", new LinkedList<>(), allServers);
	}

	public static ChooseNewMasterFailException multiMaster(List<RedisMeta> masters, List<RedisMeta> allServers){
		return new ChooseNewMasterFailException("multiMaster:" + MetaUtils.toString(masters), masters, allServers);
	}

	public static ChooseNewMasterFailException chooseNull(List<RedisMeta> allServers){
		return new ChooseNewMasterFailException("choose result null", new LinkedList<>(), allServers);
	}


}
