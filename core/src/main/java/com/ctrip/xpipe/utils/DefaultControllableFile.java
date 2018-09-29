package com.ctrip.xpipe.utils;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public class DefaultControllableFile extends AbstractControllableFile{

	public DefaultControllableFile(String file) throws IOException{
		super(file, 0);
	}
	
	public DefaultControllableFile(File file) throws IOException {
		super(file, 0);
	}

	public DefaultControllableFile(File file, long pos) throws IOException {
		super(file, pos);
	}

}
