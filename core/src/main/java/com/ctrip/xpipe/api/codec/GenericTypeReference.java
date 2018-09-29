package com.ctrip.xpipe.api.codec;

import com.fasterxml.jackson.core.type.TypeReference;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author wenchao.meng
 *
 * Aug 11, 2016
 */
public abstract class GenericTypeReference<T>
{
    protected final Type type;
    
    protected GenericTypeReference()
    {
        Type superClass = getClass().getGenericSuperclass();
        if (superClass instanceof Class<?>) { // sanity check, should never happen
            throw new IllegalArgumentException("Internal error: TypeReference constructed without actual type information");
        }
        /* 22-Dec-2008, tatu: Not sure if this case is safe -- I suspect
         *   it is possible to make it fail?
         *   But let's deal with specific
         *   case when we know an actual use case, and thereby suitable
         *   workarounds for valid case(s) and/or error to throw
         *   on invalid one(s).
         */
        type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public Type getType() { return type; }
    
    public TypeReference<T> getJacksonReference(){
    	
    	return new TypeReference<T>() {
    		@Override
    		public Type getType() {
    			return type;
    		}
		}; 
    }
}
