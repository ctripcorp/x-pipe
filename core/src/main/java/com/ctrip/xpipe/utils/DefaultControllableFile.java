package com.ctrip.xpipe.utils;

import java.io.File;

/**
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public class DefaultControllableFile extends AbstractControllableFile{

	public DefaultControllableFile(String file){
		super(file);
	}
	
	public DefaultControllableFile(File file) {
		super(file);
	}

}
