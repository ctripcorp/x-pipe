package com.ctrip.xpipe.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
public class DefaultFileConfig extends AbstractConfig{
	
	private Properties properties = new Properties();
	
	private static String DEFAULT_LOCAL_PATH = System.getProperty("localpath", "/opt/settings/xpipe");

	private String localPath;
	
	public DefaultFileConfig(){
	}

	public DefaultFileConfig(String file) {
		this(file, DEFAULT_LOCAL_PATH);
	}

	public DefaultFileConfig(String file, String localPath) {
		
		this.localPath = localPath;
		properties.putAll(load(file));
	}

	private Properties load(String file) {
		
		Properties properties = new Properties();

		//try file
		File f = new File(file);
		if(f.exists()){
			try {
				properties.load(new FileInputStream(f));
				logger.info("[load]{}", f.getAbsolutePath());
				return properties;
			} catch (IOException e) {
				throw new IllegalArgumentException("file load fail:" + file, e);
			}
		}
		
		if(localPath != null){
			f = new File(localPath + "/" + file);
			if(f.exists()){
				try {
					properties.load(new FileInputStream(f));
					logger.info("[load]{}", f.getAbsolutePath());
					return properties;
				} catch (IOException e) {
					throw new IllegalArgumentException("file load fail:" + file, e);
				}
			}
		}
		
		//try classpath
		URL url = getClass().getClassLoader().getResource(file);
		if(url != null){
			try {
				properties.load(url.openStream());
				logger.info("[load]{}", url);
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
