package com.ctrip.xpipe.redis.core.spring;

import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2021/5/18
 */
public class AbstractWebConfig implements WebMvcConfigurer {

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        List<HttpMessageConverter<?>> xmlConverters = converters.stream()
                .filter(converter -> converter instanceof MappingJackson2XmlHttpMessageConverter).collect(Collectors.toList());
        xmlConverters.forEach(converters::remove);
    }

}
