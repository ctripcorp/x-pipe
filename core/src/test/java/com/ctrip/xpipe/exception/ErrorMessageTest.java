package com.ctrip.xpipe.exception;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


/**
 * @author wenchao.meng
 *
 * Aug 11, 2016
 */
public class ErrorMessageTest extends AbstractTest{
	
	public enum ERRORCODE{
		NET_EXCEPTION,
		DB_EXCEPTION
	}

	public static class TestErrorMessage extends ErrorMessage<ERRORCODE>{
		
		public TestErrorMessage(){}
		public TestErrorMessage(ERRORCODE errorType, String errorMessage){
			super(errorType, errorMessage);
		}
		
	}
	
	@Test
	public void testSubClass(){
		
		TestErrorMessage testErrorMessage = new TestErrorMessage(ERRORCODE.DB_EXCEPTION, "hello");
		String encode = Codec.DEFAULT.encode(testErrorMessage);
	
		TestErrorMessage decode = Codec.DEFAULT.decode(encode, TestErrorMessage.class);
		
		Assert.assertEquals(testErrorMessage, decode);
		
		
	}

	@Test
	public void testSerializa() throws JsonParseException, JsonMappingException, IOException{

		ErrorMessage<ERRORCODE> error = new ErrorMessage<ERRORCODE>(ERRORCODE.NET_EXCEPTION, "conntect refused");
		
		String result = Codec.DEFAULT.encode(error);
		logger.info("{}", result);
		
		
		ObjectMapper om = new ObjectMapper();
		ErrorMessage<ERRORCODE> desr = om.readValue(result, new TypeReference<ErrorMessage<ERRORCODE>>() {
		});
		Assert.assertEquals(error, desr);
		
		desr = Codec.DEFAULT.decode(result, new GenericTypeReference<ErrorMessage<ERRORCODE>>() {
		});
		Assert.assertEquals(error, desr);
		
		//test wrong message
		try{
			String wrong = "{\"errorType\":\"NET_EXCEPTION1\",\"errorMessage\":\"conntect refused\"}";
			desr = om.readValue(wrong, new TypeReference<ErrorMessage<ERRORCODE>>() {
			});
			Assert.fail();
		}catch(Exception e){
		}
	}
	
}
