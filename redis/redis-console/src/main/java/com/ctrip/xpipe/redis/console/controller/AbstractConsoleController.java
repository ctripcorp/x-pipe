package com.ctrip.xpipe.redis.console.controller;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.spring.AbstractController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.codec.Codec;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author shyin
 *
 * Sep 2, 2016
 */
public abstract class AbstractConsoleController extends AbstractController{


	@Autowired
	protected UserInfoHolder userInfoHolder;

	protected Codec coder = Codec.DEFAULT;

	public static final String CONSOLE_PREFIX = "/console";

	@SuppressWarnings("unchecked")
	protected <T> T valueOrDefault(Class<T> clazz, T result) {
		try {
			return (null == result) ? clazz.newInstance() : result;
		} catch (InstantiationException e) {
			return (T) new Object();
		} catch (IllegalAccessException e) {
			return (T) new Object();
		}
	}
	
	protected <T> List<T> valueOrEmptySet(Class<T> clazz, List<T> result) {
		return (null == result) ? new LinkedList<T>() : result;
	}

	protected String outerDcToInnerDc(String dcId) {
		return DC_TRANSFORM_DIRECTION.OUTER_TO_INNER.transform(dcId);
	}

	protected String innerDcToOuterDc(String dcId) {
		return DC_TRANSFORM_DIRECTION.INNER_TO_OUTER.transform(dcId);
	}

	public static enum DC_TRANSFORM_DIRECTION{

		OUTER_TO_INNER,
		INNER_TO_OUTER;

		private static DcMapper dcMapper = DcMapper.INSTANCE;

		public String transform(String dc){

			if(this == OUTER_TO_INNER){
				return dcMapper.reverse(dc);
			}

			if(this == INNER_TO_OUTER){
				return dcMapper.getDc(dc);
			}

			throw new IllegalStateException("unknown diection:" + this);
		}
	}

}
