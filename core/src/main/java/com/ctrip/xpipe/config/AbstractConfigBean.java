package com.ctrip.xpipe.config;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public abstract class AbstractConfigBean implements ConfigChangeListener {

	private Config config;
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	private Map<String, Long> longCache;

	private Map<String, Integer> intergerCache;

	private Map<String, Float> floatCache;

	private AtomicLong configVersion = new AtomicLong(0L);

	public AbstractConfigBean(){
		this(Config.DEFAULT);
	}

	public AbstractConfigBean(Config config){
		this.config = config;
		config.addConfigChangeListener(this);
	}

	protected void setConfig(Config config) {
		this.config = config;
	}

	protected String getProperty(String key){
		return config.get(key);
	}

	protected String getProperty(String key, String defaultValue){
		return config.get(key, defaultValue);
	}

	protected Integer getIntProperty(String key, Integer defaultValue){
		if (intergerCache == null) {
			synchronized (this) {
				if (intergerCache == null) {
					intergerCache = Maps.newConcurrentMap();
				}
			}
		}

		return getValueFromCache(key, Functions.TO_INT_FUNCTION, intergerCache, defaultValue);
	}

	protected Long getLongProperty(String key, Long defaultValue){
		if (longCache == null) {
			synchronized (this) {
				if (longCache == null) {
					longCache = Maps.newConcurrentMap();
				}
			}
		}

		return getValueFromCache(key, Functions.TO_LONG_FUNCTION, longCache, defaultValue);
	}

	protected Float getFloatProperty(String key, Float defaultValue){
		if (floatCache == null) {
			synchronized (this) {
				if (floatCache == null) {
					floatCache = Maps.newConcurrentMap();
				}
			}
		}

		return getValueFromCache(key, Functions.TO_FLOAT_FUNCTION, floatCache, defaultValue);
	}

	protected Boolean getBooleanProperty(String key, Boolean defaultValue){
		
		String value = config.get(key);
		if(value == null){
			return defaultValue;
		}
		
		return Boolean.parseBoolean(value.trim());
	}

	protected Set<String> getSplitStringSet(String str) {
		HashSet result = new HashSet();

		String[] split = str.split("\\s*(,|;)\\s*");

		for(String sp : split){
			if(!StringUtil.isEmpty(sp)){
				result.add(sp);
			}
		}
		return result;
	}
	
	@Override
	public void onChange(String key, String oldValue, String newValue) {
		clearCache();
	}
	
	@Override
	public String toString() {
		return Codec.DEFAULT.encode(this);
	}

	private <T> T getValueFromCache(String key, Function<String, T> parser, Map<String, T> cache, T defaultValue) {
		T result = cache.get(key);

		if (result != null) {
			return result;
		}

		return getValueAndStoreToCache(key, parser, cache, defaultValue);
	}

	private <T> T getValueAndStoreToCache(String key, Function<String, T> parser, Map<String, T> cache, T defaultValue) {
		long currentConfigVersion = configVersion.get();

		T val = cache.get(key);
		String value;
		if(val == null && (value = config.get(key, null)) != null) {
			synchronized (this) {
				if(cache.get(key) == null) {
					T result = parser.apply(value);

					if (result != null) {

						if (configVersion.get() == currentConfigVersion) {
							cache.put(key, result);
							logger.info("[config updated] {}, {}", key, result);
						}

						return result;
					}

				}
				return cache.get(key);
			}
		}

		return defaultValue;
	}

	private void clearCache() {
		synchronized (this) {
			intergerCache = Maps.newConcurrentMap();
			longCache = Maps.newConcurrentMap();
			floatCache = Maps.newConcurrentMap();
			configVersion.getAndIncrement();
		}
	}

	interface Functions {
		Function<String, Integer> TO_INT_FUNCTION = new Function<String, Integer>() {
			@Override
			public Integer apply(String input) {
				return Integer.parseInt(input);
			}
		};
		Function<String, Long> TO_LONG_FUNCTION = new Function<String, Long>() {
			@Override
			public Long apply(String input) {
				return Long.parseLong(input);
			}
		};
		Function<String, Float> TO_FLOAT_FUNCTION = new Function<String, Float>() {
			@Override
			public Float apply(String input) {
				return Float.parseFloat(input);
			}
		};
		Function<String, Boolean> TO_BOOLEAN_FUNCTION = new Function<String, Boolean>() {
			@Override
			public Boolean apply(String input) {
				return Boolean.parseBoolean(input);
			}
		};
	}

}
