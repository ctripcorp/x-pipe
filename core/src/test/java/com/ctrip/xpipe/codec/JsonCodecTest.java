package com.ctrip.xpipe.codec;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.migration.OuterClientService.MigrationPublishResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Oct 25, 2016
 */
public class JsonCodecTest extends AbstractTest{
	
	@Test
	public void test(){
		
		JsonCodec jsonCodec = new JsonCodec();
		
		System.out.println(jsonCodec.encode("123\n345"));
	}
	
	@Test
	public void decodeWithCapital() {
		
		MigrationPublishResult res = new MigrationPublishResult();
		res.setMessage("test success");
		res.setSuccess(true);
		System.out.println(Codec.DEFAULT.encode(res));
		System.out.println(Codec.DEFAULT.decode("{\"Success\":true,\"Message\":\"设置成功\"}", MigrationPublishResult.class));
		System.out.println(Codec.DEFAULT.decode("{\"success\":true,\"message\":\"test\"}", MigrationPublishResult.class));
	}

	@Test
	public void testMap(){

		Map decode = JsonCodec.INSTANCE.decode("{\"a\":\"1\"}", Map.class);
		logger.info("{}", decode);

		Map<String, String> data = new HashMap();
		data.put("xpipe.sh3.ctripcorp.com", "SHAOY");
		data.put("xpipe.sh2.ctripcorp.com", "SHAJQ");

		logger.info("{}", JsonCodec.INSTANCE.encode(data));
	}

	@Test
	public void testSet(){

		Set<String> set = new HashSet<>();
		set.add("1");

		logger.info("{}", JsonCodec.INSTANCE.encode(set));
	}

	@Test
	public void testEnum(){

		Person person = new Person(SEX.FEMALE);

		logger.info("{}", JsonCodec.INSTANCE.encode(person));
	}


	public static class Person{

		private SEX sex;

		public Person(){

		}

		public Person(SEX sex){
			this.sex = sex;
		}

		public SEX getSex() {
			return sex;
		}

		public void setSex(SEX sex) {
			this.sex = sex;
		}
	}


	public enum SEX{
		MALE,
		FEMALE
	}
	
}
