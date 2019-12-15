package com.ctrip.xpipe.spring;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.util.List;

public class AbstractRestTemplateFactory {

    private static ObjectMapper createObjectMapper() {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        return objectMapper;

    }

    protected static void setXPipeSafeJacksonMapper(List<HttpMessageConverter<?>> converters) {
        for (HttpMessageConverter<?> hmc : converters) {
            if (hmc instanceof MappingJackson2HttpMessageConverter) {
                ObjectMapper objectMapper = createObjectMapper();
                MappingJackson2HttpMessageConverter mj2hmc = (MappingJackson2HttpMessageConverter) hmc;
                mj2hmc.setObjectMapper(objectMapper);
            }
        }
    }


}
