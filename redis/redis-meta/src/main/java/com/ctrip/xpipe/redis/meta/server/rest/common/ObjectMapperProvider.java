package com.ctrip.xpipe.redis.meta.server.rest.common;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;

@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {
	final ObjectMapper defaultObjectMapper;

	public ObjectMapperProvider() {
		defaultObjectMapper = createDefaultMapper();
	}

	@Override
	public ObjectMapper getContext(Class<?> type) {
		return defaultObjectMapper;
	}

	private static ObjectMapper createDefaultMapper() {
		AnnotationIntrospector jacksonIntrospector = new JacksonAnnotationIntrospector();
		ObjectMapper result = new ObjectMapper();
		result.configure(SerializationFeature.INDENT_OUTPUT, true);
		result.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		result.getDeserializationConfig().withInsertedAnnotationIntrospector(jacksonIntrospector);
		result.getSerializationConfig().withInsertedAnnotationIntrospector(jacksonIntrospector);
		return result;
	}
}
