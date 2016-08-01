package com.ctrip.xpipe.codec;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author wenchao.meng
 *
 * Jul 23, 2016
 */
public class JsonCodec extends AbstractCodec{
	
	private ObjectMapper objectMapper;
	
	public JsonCodec() {
		objectMapper = new ObjectMapper();
		
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
			throw new IllegalStateException("decode error " + data, e);
		}
	}

}
