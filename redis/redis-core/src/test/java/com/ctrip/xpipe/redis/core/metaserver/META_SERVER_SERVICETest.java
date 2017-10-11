package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.rest.ForwardType;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public class META_SERVER_SERVICETest extends AbstractRedisTest{
	
	@Test
	public void testNoMoving(){
		
		for(META_SERVER_SERVICE service : META_SERVER_SERVICE.values()){
			
			Assert.assertNotEquals(ForwardType.MOVING, service.getForwardType());
		}
	}
	
	@Test
	public void testFromPath(){
		
		for(META_SERVER_SERVICE service : META_SERVER_SERVICE.values()){
			
			String path = service.getPath();
			String realPath = replace(path);
			
			META_SERVER_SERVICE real = META_SERVER_SERVICE.fromPath(realPath, "");
			logger.info("[testFromPath]{} -> {}", realPath, real);
			Assert.assertEquals(service, real);
			
			if(path.startsWith("/")){
				real = META_SERVER_SERVICE.fromPath(realPath, "/");
				Assert.assertEquals(service, real);
			}
		}
	}

	private String replace(String path) {
		
		Matcher matcher = Pattern.compile("\\{.*?\\}").matcher(path);
		StringBuilder result = new StringBuilder();
		int begin = 0;
		while(matcher.find()){
			
			int start = matcher.start();
			int end = matcher.end();
			
			result.append(path.substring(begin, start));
			result.append(randomString(10));
			begin = end;
		}
		result.append(path.substring(begin));
		
		return result.toString();
	}
	
	@Test
	public void testReplace(){
		
		String test = "/a/{}/b/{b}/c/{ccc}";
		System.out.println(replace(test));
		
		String b = ":a:b::c:";
		System.out.println(StringUtil.join(",", b.split(":+", -1)));
		System.out.println(StringUtil.join(",", StringUtil.splitRemoveEmpty(":+", b)));
	}

}
