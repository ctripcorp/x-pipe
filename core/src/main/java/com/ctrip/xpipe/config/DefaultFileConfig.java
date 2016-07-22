package com.ctrip.xpipe.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
public class DefaultFileConfig extends AbstractConfig{
	
	private Properties properties = new Properties();
	
	public DefaultFileConfig(String file) {
		
		properties.putAll(load(file));
	}

	private Properties load(String file) {
		
		Properties properties = new Properties();

		//try file
		File f = new File(file);
		if(f.exists()){
			try {
				properties.load(new FileInputStream(f));
				return properties;
			} catch (IOException e) {
				throw new IllegalArgumentException("file load fail:" + file, e);
			}
		}
		
		//try classpath
		InputStream ins = getClass().getClassLoader().getResourceAsStream(file);
		if(ins != null){
			try {
				properties.load(ins);
			} catch (IOException e) {
				throw new IllegalArgumentException("classpath load fail:" + file, e);
			}
			return properties;
		}
		throw new IllegalArgumentException("unfound file:" + file);
	}

	@Override
	public String get(String key) {
		return properties.getProperty(key);
	}

}
