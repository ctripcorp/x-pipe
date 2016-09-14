package simpletest;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ctrip.xpipe.api.codec.Codec;

/**
 * @author shyin
 *
 * Sep 9, 2016
 */
public class CodecTest {
	Codec codec = Codec.DEFAULT;
	
	@Test
	public void testJsonDecode() {
		String toParseJson = "{\"a\":\"a\",\"b\":\"b\"}";
		
		@SuppressWarnings("unchecked")
		Map<String,String> result = codec.decode(toParseJson, Map.class);
		Map<String,String> target = new HashMap<>();
		target.put("a", "a");
		target.put("b", "b");
		
		assertEquals(result,target);
	}

}
