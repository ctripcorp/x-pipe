package com.ctrip.xpipe.codec;

import java.io.IOException;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author wenchao.meng
 *
 * Jul 23, 2016
 */
public class JsonCodec extends AbstractCodec{
	
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
		if(indent){
			objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		}
		if(privateVisible){
			objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
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
