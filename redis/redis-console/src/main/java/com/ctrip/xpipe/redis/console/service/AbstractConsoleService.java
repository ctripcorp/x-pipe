package com.ctrip.xpipe.redis.console.service;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.query.DalQueryHandler;

public abstract class AbstractConsoleService<T> {
	protected DalQueryHandler queryHandler = new DalQueryHandler();

	protected T dao;

	@SuppressWarnings("unchecked")
	@PostConstruct
    private void postConstruct() {
    	
        try {
        	Type superClass = getClass().getGenericSuperclass();
        	 if (superClass instanceof Class<?>) { // sanity check, should never happen
                 throw new IllegalArgumentException("Internal error: TypeReference constructed without actual type information");
             }
        	 Type type = ((ParameterizedType) superClass).getActualTypeArguments()[0];

            dao = (T) ContainerLoader.getDefaultContainer().lookup(parseTypeName(type.toString()));
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }
	
	private String parseTypeName(String typeString) {
		int index = typeString.indexOf("class");
		if(index >= 0) { 
			return typeString.substring(index + 5).trim();
		} else {
			throw new IllegalArgumentException("Internal error: TypeName parse failed.");
		}
	}
	
}
