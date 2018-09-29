package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.api.utils.FileSize;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public class SizeControllableFile extends AbstractControllableFile{
	
	private FileSize fileSize;

	public SizeControllableFile(File file, FileSize fileSize) throws IOException{
		super(file, 0);
		this.fileSize = fileSize;
	}

	@Override
	public long size() {
		return fileSize.getSize(() -> super.size());
	}
}
