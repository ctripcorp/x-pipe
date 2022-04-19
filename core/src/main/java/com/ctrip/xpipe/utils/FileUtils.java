package com.ctrip.xpipe.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.function.Consumer;

/**
 * @author wenchao.meng
 *
 * Aug 8, 2016
 */
public class FileUtils {
	
	private static Logger logger = LoggerFactory.getLogger(FileUtils.class);
	
	public static int DEFAULT_SHORT_PATH_LEVEL = 4;
	
	public static void recursiveDelete(File file) {
		
		if (!file.exists() || !file.canWrite()) {
			return;
		}
		if (file.isDirectory()) {
			File[] children = file.listFiles();
			if (children != null && children.length > 0) {
				for (File f : children) {
					recursiveDelete(f);
				}
			}
		}
		logger.info("[recursiveDelete]{}", shortPath(file.getAbsolutePath(), DEFAULT_SHORT_PATH_LEVEL));
		file.delete();
	}

	public static String shortPath(String absolutePath) {
		return shortPath(absolutePath, DEFAULT_SHORT_PATH_LEVEL);
	}

	public static String shortPath(String absolutePath, int level) {
		
		int index = absolutePath.length();
		for(int i=0;i<level;i++){
			int currentIndex = absolutePath.lastIndexOf(File.separator, index - 1);
			if(currentIndex < 0){
				break;
			}
			index = currentIndex;
		}
		
		if(index == absolutePath.length()){
			return absolutePath;
		}
		return absolutePath.substring(index);
	}

	public static InputStream getFileInputStream(String fileName) throws FileNotFoundException{
		
		return getFileInputStream("./", fileName, FileUtils.class);
	}

	public static InputStream getFileInputStream(String path, String fileName) throws FileNotFoundException{
		
		return getFileInputStream(path, fileName, FileUtils.class);
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
		if(url == null){
			url = clazz.getClassLoader().getResource(fileName);
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
	
	public static String readFileAsString(String fileName) throws IOException{
		
		try(InputStream ins = getFileInputStream(fileName)){ 
			String fileContent = IOUtil.toString(ins);
			return fileContent;
		}
	}

	public static void writeStringToFile(String filename, String content) throws IOException {

		try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))){
			writer.write(content);
		}
	}

	public static void readFileAsStringLineByLine(File file, Consumer<String> consumer) {
		try(FileInputStream fileInputStream = new FileInputStream(file)) {
			try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream))) {
				String str;
				while (null != (str = bufferedReader.readLine())) {
					consumer.accept(str);
				}
			}
		} catch (FileNotFoundException fileNotFoundExc) {
			logger.info("[readFileAsStringLineByLine][{}] file not found", file);
		} catch (IOException ioExc) {
			logger.info("[readFileAsStringLineByLine][{}] io err", file, ioExc);
		} catch (Throwable th) {
			logger.info("[readFileAsStringLineByLine][{}] fail", file, th);
		}
	}

}
