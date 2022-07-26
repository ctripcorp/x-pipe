package com.ctrip.xpipe.codec;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jul 23, 2016
 */
public class JsonCodec extends AbstractCodec{
	
	public static JsonCodec INSTANCE = new JsonCodec(); 
	
	private ObjectMapper objectMapper;
	
	public JsonCodec() {
		this(false, false);
	}

	public JsonCodec(boolean indent){
		this(indent, false);
	}

	public JsonCodec(boolean indent, boolean privateVisible){

		objectMapper = new ObjectMapper();
		objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		
		if(indent){
			objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		}
		if(privateVisible){
			objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
			objectMapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
			objectMapper.setVisibility(PropertyAccessor.IS_GETTER, Visibility.NONE);
		}
	}


	@Override
	public String encode(Object obj) {
		try {
			return objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException("encode error " + obj, e);
		}
	}

	@Override
	public <T> T decode(String data, Class<T> clazz) {
		
		try {
			return objectMapper.readValue(data, clazz);
		} catch (IOException e) {
			throw new IllegalStateException("decode error " + data, e);
		}
	}


	@Override
	public byte[] encodeAsBytes(Object obj) {
		try {
			return objectMapper.writeValueAsBytes(obj);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException("encode error " + obj, e);
		}
	}

	@Override
	public <T> T decode(byte[] data, Class<T> clazz) {
		try {
			return objectMapper.readValue(data, clazz);
		} catch (IOException e) {
			throw new IllegalStateException("decode error " + new String(data), e);
		}
	}

	@Override
	public <T> T decode(String data, GenericTypeReference<T> reference) {
		try {
			return objectMapper.readValue(data, reference.getJacksonReference());
		} catch (IOException e) {
			throw new IllegalStateException("decode error " + data, e);
		}
	}

	@Override
	public <T> T decode(byte[] data, GenericTypeReference<T> reference) {
		
		try {
			return objectMapper.readValue(data, reference.getJacksonReference());
		} catch (IOException e) {
			throw new IllegalStateException("decode error " + new String(data), e);
		}
	}
}
