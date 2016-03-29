package com.ctrip.xpipe.api.codec;

import java.nio.charset.Charset;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:29:12
 */
public interface Codec {

	public static final Charset defaultCharset = Charset.forName("UTF-8");
	
}
