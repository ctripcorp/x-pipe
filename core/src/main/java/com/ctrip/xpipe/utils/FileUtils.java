package com.ctrip.xpipe.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Aug 8, 2016
 */
public class FileUtils {
	
	private static Logger logger = LoggerFactory.getLogger(FileUtils.class); 
	
	public static InputStream getFileInputStream(String fileName) throws FileNotFoundException{
		
		return getFileInputStream("./", fileName, FileUtils.class);
	}

	public static InputStream getFileInputStream(String path, String fileName) throws FileNotFoundException{
		
		return getFileInputStream("./", fileName, FileUtils.class);
	}

	public static InputStream getFileInputStream(String fileName, Class<?> clazz) throws FileNotFoundException{
		
		return getFileInputStream("./", fileName, clazz);
	}

	public static InputStream getFileInputStream(String path, String fileName, Class<?> clazz) throws FileNotFoundException{
		
		File f = null;
		if(path != null){
			f = new File(path+ "/" + fileName);
			if(f.exists()){
				try {
					logger.info("[getFileInputStream]{}", f.getAbsolutePath());
					return new FileInputStream(f);
				} catch (IOException e) {
					throw new IllegalArgumentException("file load fail:" + f, e);
				}
			}
		}

		//try file
		f = new File(fileName);
		if(f.exists()){
			try {
				logger.info("[getFileInputStream]{}", f.getAbsolutePath());
				return new FileInputStream(f);
			} catch (IOException e) {
				throw new IllegalArgumentException("file load fail:" + f, e);
			}
		}
		
		//try classpath
		URL url = clazz.getResource(fileName);
		logger.info("[tryResource]{}", url);
		if(url == null){
			url = clazz.getClassLoader().getResource(fileName);
			logger.info("[tryResourceClassLoader]{}", url);
		}
		if(url != null){
			try {
				logger.info("[load]{}", url);
				return url.openStream();
			} catch (IOException e) {
				throw new IllegalArgumentException("classpath load fail:" + url, e);
			}
		}

		throw new FileNotFoundException(path + ","  + fileName);
	}

}
