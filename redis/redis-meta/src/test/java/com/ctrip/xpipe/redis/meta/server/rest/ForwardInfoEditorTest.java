package com.ctrip.xpipe.redis.meta.server.rest;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.rest.ForwardType;
import org.junit.Assert;
import org.junit.Test;



/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class ForwardInfoEditorTest extends AbstractMetaServerTest{
	
	@Test
	public void  testEditor(){
		
		ForwardInfo info = new ForwardInfo(ForwardType.FORWARD, 100);
		
		ForwardInfoEditor editor = new ForwardInfoEditor();
		editor.setValue(info);
		
		String result = editor.getAsText();
		
		Assert.assertEquals(Codec.DEFAULT.encode(info), result);
		
		editor.setAsText(result);
	}

	
}
